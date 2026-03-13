"""
ETL Pipeline — MSPR BLOC 3
Fichiers traités :
  - Premier_tour_rhone_sep.csv        → fact_election_tour1
  - Second_tour_rhone_sep.csv         → fact_election_tour2
  - donnee-dep-...csv                 → fact_securite
  - emploi_et_pop-active_2022.CSV     → fact_emploi_pop_active   (filtre Rhône 69)
  - data_revenus_2021.csv             → fact_revenus             (filtre Rhône 69)
  - diplomes_et_formation-2022.CSV    → fact_diplomes            (filtre Rhône 69)
  - logement-2022.CSV                 → fact_logement            (filtre Rhône 69)
  - contours-iris-metropole-lyon.csv  → dim_iris_lyon            (déjà filtré Lyon)
"""

import os
import yaml
import pandas as pd
import duckdb


# ════════════════════════════════════════════════════════════════════════════
# ETL
# ════════════════════════════════════════════════════════════════════════════

class ETL:

    def __init__(self, config_path: str = "config.yaml"):
        # Cherche le config.yaml dans le même dossier que ce script
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cfg_full = os.path.join(base_dir, config_path)

        with open(cfg_full, encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)

        self.base       = self.cfg["data_path"]
        self.files      = self.cfg["files"]
        self.out_folder = self.cfg["output_path"]
        self.db_path    = self.cfg.get("db_path", "warehouse.duckdb")

        os.makedirs(self.out_folder, exist_ok=True)

    # ────────────────────────────────────────────────────────────────────────
    # HELPERS INTERNES
    # ────────────────────────────────────────────────────────────────────────

    def _read(self, filename: str, encoding: str = "utf-8") -> pd.DataFrame:
        """Lecture CSV sep=';', tout en str pour éviter les warnings de type."""
        path = os.path.join(self.base, filename)
        return pd.read_csv(path, sep=";", encoding=encoding,
                           dtype=str, low_memory=False)

    @staticmethod
    def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
        """Noms de colonnes → minuscules, sans accents parasites, _ comme séparateur."""
        df = df.copy()
        df.columns = (
            df.columns
            .str.strip()
            .str.lstrip("\ufeff")          # supprime le BOM UTF-8
            .str.strip('"')               # supprime les guillemets résiduels
            .str.lower()
            .str.replace(r"[\s\-/]+", "_", regex=True)
            .str.replace(r"[^\w]", "",    regex=True)
        )
        return df

    @staticmethod
    def _to_num(series: pd.Series) -> pd.Series:
        """Convertit une série str en numérique (virgule → point)."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", ".", regex=False).str.strip(),
            errors="coerce"
        )

    @staticmethod
    def _is_rhone(code: str) -> bool:
        """True si le code INSEE appartient au département 69 (Rhône)."""
        code = str(code).strip()
        return code.startswith("69") and len(code) >= 5

    # ────────────────────────────────────────────────────────────────────────
    # EXTRACT
    # ────────────────────────────────────────────────────────────────────────

    def extract(self) -> dict:
        print("\n── EXTRACT ──────────────────────────────────────")
        data = {
            "t1"       : self._read(self.files["t1"],       encoding="utf-8"),
            "t2"       : self._read(self.files["t2"],       encoding="utf-8"),
            "securite" : self._read(self.files["securite"], encoding="latin1"),
            "emploi"   : self._read(self.files["emploi"],   encoding="latin1"),
            "revenus"  : self._read(self.files["revenus"],  encoding="latin1"),
            "diplomes" : self._read(self.files["diplomes"], encoding="latin1"),
            "logement" : self._read(self.files["logement"], encoding="latin1"),
            "iris"     : self._read(self.files["iris"],     encoding="utf-8"),
        }
        for k, df in data.items():
            print(f"  {k:12s} → {len(df):>6} lignes brutes")
        return data

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Élections
    # ────────────────────────────────────────────────────────────────────────

    def clean_election(self, df: pd.DataFrame, label: str) -> pd.DataFrame:
        """
        Nettoyage commun tour 1 & tour 2.
        Colonnes attendues : inscrits, abstentions, votants, blancs, nuls, exprimes
        + colonnes ratio_* déjà calculées dans la source.
        """
        df = self._norm_cols(df)

        # Suppression des colonnes canton (vides dans le Rhône)
        for col in ["code_canton", "libelle_canton"]:
            if col in df.columns and df[col].isna().all():
                df = df.drop(columns=[col])

        # Conversion numérique des comptages et ratios
        vote_cols  = ["inscrits", "abstentions", "votants", "blancs", "nuls", "exprimes"]
        ratio_cols = [c for c in df.columns if c.startswith("ratio_")]
        for col in vote_cols + ratio_cols:
            if col in df.columns:
                df[col] = self._to_num(df[col])

        # Intégrité : on garde uniquement les lignes cohérentes
        df = df.dropna(subset=["inscrits"])
        df = df[df["votants"] <= df["inscrits"]]
        df = df[df["exprimes"] == (df["votants"] - df["blancs"] - df["nuls"])]

        # Nettoyage texte
        for col in ["libelle_commune", "libelle_circonscription", "libelle_departement"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()

        # Codes géo
        df["code_commune"]       = df["code_commune"].astype(str).str.strip().str.zfill(5)
        df["code_departement"]   = df["code_departement"].astype(str).str.strip().str.zfill(2)

        df = df.drop_duplicates()

        print(f"  {label:12s} → {len(df):>6} lignes propres")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Sécurité
    # ────────────────────────────────────────────────────────────────────────

    def clean_securite(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Données criminalité par département.
        Filtre sur département 69 (Rhône).
        """
        df = self._norm_cols(df)

        # La 1ère colonne contient le code département (avec BOM/guillemets nettoyés par _norm_cols)
        dept_col = df.columns[0]  # → 'code_departement'
        df = df.rename(columns={dept_col: "code_departement"})

        # Nettoyage code département
        df["code_departement"] = (
            df["code_departement"].astype(str).str.strip().str.strip('"').str.zfill(2)
        )
        df["code_region"] = df["code_region"].astype(str).str.strip().str.zfill(2)

        # Filtre Rhône
        df = df[df["code_departement"] == "69"].copy()

        # Numérique
        for col in ["nombre", "taux_pour_mille", "insee_pop", "insee_log"]:
            if col in df.columns:
                df[col] = self._to_num(df[col])

        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

        df = df.drop_duplicates()

        print(f"  securite    → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Emploi & population active
    # ────────────────────────────────────────────────────────────────────────

    def clean_emploi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Emploi et population active 2022 — niveau commune.
        Filtre département 69.
        """
        df = self._norm_cols(df)

        # Filtre Rhône
        df = df[df["codgeo"].apply(self._is_rhone)].copy()

        # Toutes les colonnes sauf codgeo → numérique
        for col in df.columns:
            if col != "codgeo":
                df[col] = self._to_num(df[col])

        df = df.dropna(how="all", subset=[c for c in df.columns if c != "codgeo"])
        df = df.drop_duplicates(subset=["codgeo"])

        print(f"  emploi      → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Revenus
    # ────────────────────────────────────────────────────────────────────────

    def clean_revenus(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Revenus 2021 — niveau commune/IRIS.
        Filtre département 69.
        """
        df = self._norm_cols(df)

        # Filtre Rhône
        df = df[df["codgeo"].apply(self._is_rhone)].copy()

        # Virgules décimales + numérique
        for col in df.columns:
            if col != "codgeo":
                df[col] = self._to_num(df[col])

        df = df.drop_duplicates(subset=["codgeo"])

        print(f"  revenus     → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Diplômes & formation
    # ────────────────────────────────────────────────────────────────────────

    def clean_diplomes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Diplômes 2022 — niveau IRIS.
        Filtre département 69.
        """
        df = self._norm_cols(df)

        # Renommage clés
        df = df.rename(columns={"iris": "codeiris", "com": "codgeo"})

        # Nettoyage caractères parasites dans les noms de colonnes (ex: \x96)
        df.columns = [c.encode("ascii", "ignore").decode() for c in df.columns]
        df.columns = pd.Index([c.strip("_") for c in df.columns])

        # Filtre Rhône
        df = df[df["codgeo"].astype(str).str.strip().apply(self._is_rhone)].copy()

        # Numérique sauf clés texte
        key_cols = {"codeiris", "codgeo", "typ_iris", "lab_iris"}
        for col in df.columns:
            if col not in key_cols:
                df[col] = self._to_num(df[col])

        num_cols = [c for c in df.columns if c not in key_cols]
        df = df.dropna(how="all", subset=num_cols)
        df = df.drop_duplicates(subset=["codeiris"])

        print(f"  diplomes    → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Logement
    # ────────────────────────────────────────────────────────────────────────

    def clean_logement(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Logement 2022 — niveau IRIS.
        Filtre département 69.
        """
        df = self._norm_cols(df)

        # Renommage clés
        df = df.rename(columns={"iris": "codeiris", "com": "codgeo"})

        # Filtre Rhône
        df = df[df["codgeo"].astype(str).str.strip().apply(self._is_rhone)].copy()

        # Numérique sauf clés texte
        key_cols = {"codeiris", "codgeo", "typ_iris", "lab_iris"}
        for col in df.columns:
            if col not in key_cols:
                df[col] = self._to_num(df[col])

        num_cols = [c for c in df.columns if c not in key_cols]
        df = df.dropna(how="all", subset=num_cols)
        df = df.drop_duplicates(subset=["codeiris"])

        print(f"  logement    → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Contours IRIS (métropole Lyon)
    # ────────────────────────────────────────────────────────────────────────

    def clean_iris(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Contours IRIS métropole de Lyon.
        Déjà filtré sur Lyon → nettoyage et standardisation uniquement.
        """
        df = self._norm_cols(df)

        # Conversion types
        df["codeiris"] = df["codeiris"].astype(str).str.strip()
        df["insee"]    = df["insee"].astype(str).str.strip()
        df["gid"]      = pd.to_numeric(df["gid"], errors="coerce")

        for col in ["libelle", "commune", "typelibelle"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()

        if "type" in df.columns:
            df["type"] = df["type"].astype(str).str.strip().str.upper()

        df = df.drop_duplicates(subset=["codeiris"])

        print(f"  iris        → {len(df):>6} lignes propres (métropole Lyon)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # LOAD
    # ────────────────────────────────────────────────────────────────────────

    def _save_csv(self, df: pd.DataFrame, filename: str) -> None:
        path = os.path.join(self.out_folder, filename)
        df.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
        print(f"  CSV  → {filename}  ({len(df)} lignes)")

    def _save_duckdb(self, df: pd.DataFrame, table: str) -> None:
        con = duckdb.connect(self.db_path)
        con.register("_tmp", df)
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _tmp")
        con.close()
        print(f"  DuckDB → {table}")

    def load(self, tables: dict) -> None:
        print("\n── LOAD ─────────────────────────────────────────")
        for table_name, (df, csv_name) in tables.items():
            self._save_csv(df, csv_name)
            self._save_duckdb(df, table_name)

    # ────────────────────────────────────────────────────────────────────────
    # RUN
    # ────────────────────────────────────────────────────────────────────────

    def run(self) -> None:

        # ── Extract ────────────────────────────────────────────────────────
        raw = self.extract()

        # ── Transform ──────────────────────────────────────────────────────
        print("\n── TRANSFORM ────────────────────────────────────")

        clean = {
            "fact_election_tour1"    : (
                self.clean_election(raw["t1"], "tour1"),
                "clean_election_tour1.csv"
            ),
            "fact_election_tour2"    : (
                self.clean_election(raw["t2"], "tour2"),
                "clean_election_tour2.csv"
            ),
            "fact_securite"          : (
                self.clean_securite(raw["securite"]),
                "clean_securite.csv"
            ),
            "fact_emploi_pop_active" : (
                self.clean_emploi(raw["emploi"]),
                "clean_emploi_pop_active.csv"
            ),
            "fact_revenus"           : (
                self.clean_revenus(raw["revenus"]),
                "clean_revenus.csv"
            ),
            "fact_diplomes"          : (
                self.clean_diplomes(raw["diplomes"]),
                "clean_diplomes.csv"
            ),
            "fact_logement"          : (
                self.clean_logement(raw["logement"]),
                "clean_logement.csv"
            ),
            "dim_iris_lyon"          : (
                self.clean_iris(raw["iris"]),
                "clean_iris_lyon.csv"
            ),
        }

        # ── Load ───────────────────────────────────────────────────────────
        self.load(clean)

        print("\n✔  ETL terminé avec succès !")
        print(f"   CSV     → {self.out_folder}")
        print(f"   DuckDB  → {self.db_path}")


# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    etl = ETL()
    etl.run()