# """
# ETL Pipeline — MSPR BLOC 3
# Version enrichie avec entrepôt décisionnel simple (DuckDB)

# Sources :
#   - Premier_tour_rhone_sep.csv        → fact_election_tour1
#   - Second_tour_rhone_sep.csv         → fact_election_tour2
#   - donnee-dep-...csv                 → fact_securite
#   - emploi_et_pop-active_2022.CSV     → fact_emploi_pop_active
#   - data_revenus_2021.csv             → fact_revenus
#   - diplomes_et_formation-2022.CSV    → fact_diplomes
#   - logement-2022.CSV                 → fact_logement
#   - contours-iris-metropole-lyon.csv  → dim_iris_lyon

# Dimensions construites :
#   - dim_commune
#   - dim_departement
#   - dim_temps

# Sorties :
#   - CSV nettoyés
#   - Tables DuckDB
#   - Rapport qualité CSV
#   - Vue analytique SQL
# """

# import os
# import logging
# from datetime import datetime

# import unicodedata

# import yaml
# import pandas as pd
# import duckdb


# # ════════════════════════════════════════════════════════════════════════════
# # ETL
# # ════════════════════════════════════════════════════════════════════════════

# class ETL:

#     def __init__(self, config_path: str = "config.yaml"):
#         base_dir = os.path.dirname(os.path.abspath(__file__))
#         cfg_full = os.path.join(base_dir, config_path)

#         if not os.path.exists(cfg_full):
#             raise FileNotFoundError(f"config.yaml introuvable : {cfg_full}")

#         with open(cfg_full, encoding="utf-8") as f:
#             self.cfg = yaml.safe_load(f)

#         self.base = self.cfg["data_path"]
#         self.files = self.cfg["files"]
#         self.out_folder = self.cfg["output_path"]
#         self.db_path = self.cfg.get("db_path", "warehouse.duckdb")
#         self.log_path = self.cfg.get("log_path", "etl.log")

#         os.makedirs(self.out_folder, exist_ok=True)

#         self.quality_rows = []
#         self._setup_logger()

#     # ────────────────────────────────────────────────────────────────────────
#     # LOGGER
#     # ────────────────────────────────────────────────────────────────────────

#     def _setup_logger(self) -> None:
#         logging.basicConfig(
#             filename=self.log_path,
#             level=logging.INFO,
#             format="%(asctime)s | %(levelname)s | %(message)s",
#             encoding="utf-8"
#         )
#         logging.info("=== Démarrage ETL ===")

#     def _log(self, msg: str) -> None:
#         print(msg)
#         logging.info(msg)

#     # ────────────────────────────────────────────────────────────────────────
#     # HELPERS INTERNES
#     # ────────────────────────────────────────────────────────────────────────

#     def _read(self, filename: str, encoding: str = "utf-8") -> pd.DataFrame:
#         path = os.path.join(self.base, filename)

#         if not os.path.exists(path):
#             raise FileNotFoundError(f"Fichier introuvable : {path}")

#         df = pd.read_csv(
#             path,
#             sep=";",
#             encoding=encoding,
#             dtype=str,
#             low_memory=False
#         )
#         return df

#     @staticmethod
#     def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
#         df = df.copy()
#         df.columns = (
#             df.columns
#             .str.strip()
#             .str.lstrip("\ufeff")
#             .str.strip('"')
#             .str.lower()
#             .str.replace(r"[\s\-/]+", "_", regex=True)
#             .str.replace(r"[^\w]", "", regex=True)
#         )
#         return df

#     @staticmethod
#     def _to_num(series: pd.Series) -> pd.Series:
#         return pd.to_numeric(
#             series.astype(str)
#             .str.replace(",", ".", regex=False)
#             .str.strip(),
#             errors="coerce"
#         )

#     @staticmethod
#     def _is_rhone(code: str) -> bool:
#         code = str(code).strip()
#         return code.startswith("69") and len(code) >= 5

#     @staticmethod
#     def _safe_str(series: pd.Series, zfill: int | None = None) -> pd.Series:
#         s = series.astype(str).str.strip()
#         if zfill is not None:
#             s = s.str.zfill(zfill)
#         return s

#     @staticmethod
#     def _repair_mojibake_text(value):
#         if pd.isna(value):
#             return value

#         text = str(value).strip()
#         if text == "" or text.lower() == "nan":
#             return pd.NA

#         if any(ch in text for ch in ("Ã", "Â", "â", "ð", "�")):
#             try:
#                 text = text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
#             except:
#                 pass

#         return text


#     def _clean_text_series(self, series: pd.Series, remove_accents: bool = False) -> pd.Series:
#         s = series.apply(self._repair_mojibake_text)
#         s = s.astype("string").str.strip().str.title()

#         s = s.replace({
#             "Rhone": "Rhône",
#             "1Ere": "1ère",
#             "2Eme": "2ème",
#             "3Eme": "3ème",
#             "4Eme": "4ème",
#             "5Eme": "5ème",
#             "6Eme": "6ème",
#             "7Eme": "7ème",
#             "8Eme": "8ème",
#             "9Eme": "9ème",
#         })

#         s = s.str.replace(r"(\d+)Ème", r"\1ème", regex=True)

#         if remove_accents:
#             s = s.apply(
#                 lambda x: unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode("ascii")
#                 if pd.notna(x) else x
#             )

#         return s

#     def _add_quality_row(
#         self,
#         table_name: str,
#         rows_before: int,
#         rows_after: int,
#         cols_count: int,
#         null_cells: int,
#         duplicates_removed: int
#     ) -> None:
#         self.quality_rows.append({
#             "table_name": table_name,
#             "rows_before": rows_before,
#             "rows_after": rows_after,
#             "rows_removed": rows_before - rows_after,
#             "columns_count": cols_count,
#             "null_cells": null_cells,
#             "duplicates_removed": duplicates_removed,
#         })

#     def _require_cols(self, df: pd.DataFrame, required_cols: list[str], table_name: str) -> None:
#         missing = [c for c in required_cols if c not in df.columns]
#         if missing:
#             raise ValueError(
#                 f"Colonnes manquantes dans {table_name} : {missing}. "
#                 f"Colonnes présentes : {list(df.columns)}"
#             )

#     # ────────────────────────────────────────────────────────────────────────
#     # EXTRACT
#     # ────────────────────────────────────────────────────────────────────────

#     def extract(self) -> dict:
#         self._log("\n── EXTRACT ──────────────────────────────────────")

#         data = {
#             "t1": self._read(self.files["t1"], encoding="utf-8"),
#             "t2": self._read(self.files["t2"], encoding="utf-8"),
#             "securite": self._read(self.files["securite"], encoding="latin1"),
#             "emploi": self._read(self.files["emploi"], encoding="latin1"),
#             "revenus": self._read(self.files["revenus"], encoding="latin1"),
#             "diplomes": self._read(self.files["diplomes"], encoding="latin1"),
#             "logement": self._read(self.files["logement"], encoding="latin1"),
#             "iris": self._read(self.files["iris"], encoding="utf-8"),
#         }

#         for k, df in data.items():
#             self._log(f"  {k:12s} → {len(df):>6} lignes brutes")

#         return data

#     # ────────────────────────────────────────────────────────────────────────
#     # TRANSFORM — Élections
#     # ────────────────────────────────────────────────────────────────────────

#     def clean_election(self, df: pd.DataFrame, label: str) -> pd.DataFrame:
#         rows_before = len(df)
#         df = self._norm_cols(df)

#         self._require_cols(
#             df,
#             ["code_commune", "code_departement", "inscrits", "votants", "blancs", "nuls", "exprimes"],
#             f"election_{label}"
#         )

#         for col in ["code_canton", "libelle_canton"]:
#             if col in df.columns and df[col].isna().all():
#                 df = df.drop(columns=[col])

#         vote_cols = ["inscrits", "abstentions", "votants", "blancs", "nuls", "exprimes"]
#         ratio_cols = [c for c in df.columns if c.startswith("ratio_")]

#         for col in vote_cols + ratio_cols:
#             if col in df.columns:
#                 df[col] = self._to_num(df[col])

#         df = df.dropna(subset=["inscrits"])
#         df = df[df["votants"] <= df["inscrits"]]
#         df = df[df["exprimes"] == (df["votants"] - df["blancs"] - df["nuls"])]

#         text_cols = [
#             "libelle_commune",
#             "libelle_circonscription",
#             "libelle_departement",
#             "libelle_region"
#         ]

#         for col in text_cols:
#             if col in df.columns:
#                 df[col] = self._clean_text_series(df[col])

#         for col in text_cols:
#             if col in df.columns:
#                 df[col] = self._clean_text_series(df[col], remove_accents=True)

#         df["code_commune"] = self._safe_str(df["code_commune"], 5)
#         df["code_departement"] = self._safe_str(df["code_departement"], 2)

#         if "code_circonscription" in df.columns:
#             df["code_circonscription"] = self._safe_str(df["code_circonscription"])
        
#         if "id_brut_miom" in df.columns:
#             df["id_brut_miom"] = (
#                 df["id_brut_miom"]
#                 .astype(str)
#                 .str.strip()
#                 .str.replace("_", "", regex=False)
#     )

#         dup_before = len(df)
#         df = df.drop_duplicates()
#         duplicates_removed = dup_before - len(df)

#         self._add_quality_row(
#             table_name=f"fact_election_{label}",
#             rows_before=rows_before,
#             rows_after=len(df),
#             cols_count=len(df.columns),
#             null_cells=int(df.isna().sum().sum()),
#             duplicates_removed=duplicates_removed
#         )

#         self._log(f"  {label:12s} → {len(df):>6} lignes propres")
#         return df

#     # ────────────────────────────────────────────────────────────────────────
#     # TRANSFORM — Sécurité
#     # ────────────────────────────────────────────────────────────────────────

#     def clean_securite(self, df: pd.DataFrame) -> pd.DataFrame:
#         rows_before = len(df)
#         df = self._norm_cols(df)

#         dept_col = df.columns[0]
#         df = df.rename(columns={dept_col: "code_departement"})

#         self._require_cols(
#             df,
#             ["code_departement", "code_region", "annee"],
#             "fact_securite"
#         )

#         df["code_departement"] = (
#             df["code_departement"].astype(str).str.strip().str.strip('"').str.zfill(2)
#         )
#         df["code_region"] = df["code_region"].astype(str).str.strip().str.zfill(2)

#         df = df[df["code_departement"] == "69"].copy()

#         for col in ["nombre", "taux_pour_mille", "insee_pop", "insee_log"]:
#             if col in df.columns:
#                 df[col] = self._to_num(df[col])

#         df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

#         dup_before = len(df)
#         df = df.drop_duplicates()
#         duplicates_removed = dup_before - len(df)

#         self._add_quality_row(
#             table_name="fact_securite",
#             rows_before=rows_before,
#             rows_after=len(df),
#             cols_count=len(df.columns),
#             null_cells=int(df.isna().sum().sum()),
#             duplicates_removed=duplicates_removed
#         )

#         self._log(f"  securite    → {len(df):>6} lignes propres (Rhône)")
#         return df

#     # ────────────────────────────────────────────────────────────────────────
#     # TRANSFORM — Emploi
#     # ────────────────────────────────────────────────────────────────────────

#     def clean_emploi(self, df: pd.DataFrame) -> pd.DataFrame:
#         rows_before = len(df)
#         df = self._norm_cols(df)

#         self._require_cols(df, ["codgeo"], "fact_emploi_pop_active")

#         df = df[df["codgeo"].apply(self._is_rhone)].copy()
#         df["codgeo"] = self._safe_str(df["codgeo"], 5)

#         for col in df.columns:
#             if col != "codgeo":
#                 df[col] = self._to_num(df[col])

#         df = df.dropna(how="all", subset=[c for c in df.columns if c != "codgeo"])

#         dup_before = len(df)
#         df = df.drop_duplicates(subset=["codgeo"])
#         duplicates_removed = dup_before - len(df)

#         self._add_quality_row(
#             table_name="fact_emploi_pop_active",
#             rows_before=rows_before,
#             rows_after=len(df),
#             cols_count=len(df.columns),
#             null_cells=int(df.isna().sum().sum()),
#             duplicates_removed=duplicates_removed
#         )

#         self._log(f"  emploi      → {len(df):>6} lignes propres (Rhône)")
#         return df

#     # ────────────────────────────────────────────────────────────────────────
#     # TRANSFORM — Revenus
#     # ────────────────────────────────────────────────────────────────────────

#     def clean_revenus(self, df: pd.DataFrame) -> pd.DataFrame:
#         rows_before = len(df)
#         df = self._norm_cols(df)

#         self._require_cols(df, ["codgeo"], "fact_revenus")

#         df = df[df["codgeo"].apply(self._is_rhone)].copy()
#         df["codgeo"] = self._safe_str(df["codgeo"], 5)

#         for col in df.columns:
#             if col != "codgeo":
#                 df[col] = self._to_num(df[col])

#         dup_before = len(df)
#         df = df.drop_duplicates(subset=["codgeo"])
#         duplicates_removed = dup_before - len(df)

#         self._add_quality_row(
#             table_name="fact_revenus",
#             rows_before=rows_before,
#             rows_after=len(df),
#             cols_count=len(df.columns),
#             null_cells=int(df.isna().sum().sum()),
#             duplicates_removed=duplicates_removed
#         )

#         self._log(f"  revenus     → {len(df):>6} lignes propres (Rhône)")
#         return df

#     # ────────────────────────────────────────────────────────────────────────
#     # TRANSFORM — Diplômes
#     # ────────────────────────────────────────────────────────────────────────

#     def clean_diplomes(self, df: pd.DataFrame) -> pd.DataFrame:
#         rows_before = len(df)
#         df = self._norm_cols(df)
#         df = df.rename(columns={"iris": "codeiris", "com": "codgeo"})

#         df.columns = [c.encode("ascii", "ignore").decode() for c in df.columns]
#         df.columns = pd.Index([c.strip("_") for c in df.columns])

#         self._require_cols(df, ["codeiris", "codgeo"], "fact_diplomes")

#         df["codgeo"] = df["codgeo"].astype(str).str.strip()
#         df = df[df["codgeo"].apply(self._is_rhone)].copy()

#         df["codgeo"] = self._safe_str(df["codgeo"], 5)
#         df["codeiris"] = self._safe_str(df["codeiris"])

#         key_cols = {"codeiris", "codgeo", "typ_iris", "lab_iris"}
#         for col in df.columns:
#             if col not in key_cols:
#                 df[col] = self._to_num(df[col])

#         num_cols = [c for c in df.columns if c not in key_cols]
#         df = df.dropna(how="all", subset=num_cols)

#         dup_before = len(df)
#         df = df.drop_duplicates(subset=["codeiris"])
#         duplicates_removed = dup_before - len(df)

#         self._add_quality_row(
#             table_name="fact_diplomes",
#             rows_before=rows_before,
#             rows_after=len(df),
#             cols_count=len(df.columns),
#             null_cells=int(df.isna().sum().sum()),
#             duplicates_removed=duplicates_removed
#         )

#         self._log(f"  diplomes    → {len(df):>6} lignes propres (Rhône)")
#         return df

#     # ────────────────────────────────────────────────────────────────────────
#     # TRANSFORM — Logement
#     # ────────────────────────────────────────────────────────────────────────

#     def clean_logement(self, df: pd.DataFrame) -> pd.DataFrame:
#         rows_before = len(df)
#         df = self._norm_cols(df)
#         df = df.rename(columns={"iris": "codeiris", "com": "codgeo"})

#         self._require_cols(df, ["codeiris", "codgeo"], "fact_logement")

#         df["codgeo"] = df["codgeo"].astype(str).str.strip()
#         df = df[df["codgeo"].apply(self._is_rhone)].copy()

#         df["codgeo"] = self._safe_str(df["codgeo"], 5)
#         df["codeiris"] = self._safe_str(df["codeiris"])

#         key_cols = {"codeiris", "codgeo", "typ_iris", "lab_iris"}
#         for col in df.columns:
#             if col not in key_cols:
#                 df[col] = self._to_num(df[col])

#         num_cols = [c for c in df.columns if c not in key_cols]
#         df = df.dropna(how="all", subset=num_cols)

#         dup_before = len(df)
#         df = df.drop_duplicates(subset=["codeiris"])
#         duplicates_removed = dup_before - len(df)

#         self._add_quality_row(
#             table_name="fact_logement",
#             rows_before=rows_before,
#             rows_after=len(df),
#             cols_count=len(df.columns),
#             null_cells=int(df.isna().sum().sum()),
#             duplicates_removed=duplicates_removed
#         )

#         self._log(f"  logement    → {len(df):>6} lignes propres (Rhône)")
#         return df

#     # ────────────────────────────────────────────────────────────────────────
#     # TRANSFORM — IRIS Lyon
#     # ────────────────────────────────────────────────────────────────────────

#     def clean_iris(self, df: pd.DataFrame) -> pd.DataFrame:
#         rows_before = len(df)
#         df = self._norm_cols(df)

#         self._require_cols(df, ["codeiris"], "dim_iris_lyon")

#         df["codeiris"] = df["codeiris"].astype(str).str.strip()

#         if "insee" in df.columns:
#             df["insee"] = df["insee"].astype(str).str.strip()
#         if "gid" in df.columns:
#             df["gid"] = pd.to_numeric(df["gid"], errors="coerce")

#         for col in ["libelle", "commune", "typelibelle"]:
#             if col in df.columns:
#                 df[col] = df[col].astype(str).str.strip().str.title()

#         if "type" in df.columns:
#             df["type"] = df["type"].astype(str).str.strip().str.upper()

#         dup_before = len(df)
#         df = df.drop_duplicates(subset=["codeiris"])
#         duplicates_removed = dup_before - len(df)

#         self._add_quality_row(
#             table_name="dim_iris_lyon",
#             rows_before=rows_before,
#             rows_after=len(df),
#             cols_count=len(df.columns),
#             null_cells=int(df.isna().sum().sum()),
#             duplicates_removed=duplicates_removed
#         )

#         self._log(f"  iris        → {len(df):>6} lignes propres (métropole Lyon)")
#         return df

#     # ────────────────────────────────────────────────────────────────────────
#     # BUILD DIMENSIONS
#     # ────────────────────────────────────────────────────────────────────────

#     def build_dim_commune(
#         self,
#         df_t1: pd.DataFrame,
#         df_t2: pd.DataFrame,
#         df_emploi: pd.DataFrame,
#         df_revenus: pd.DataFrame
#     ) -> pd.DataFrame:
#         frames = []

#         if {"code_commune", "libelle_commune", "code_departement"}.issubset(df_t1.columns):
#             frames.append(
#                 df_t1[["code_commune", "libelle_commune", "code_departement"]]
#                 .rename(columns={"code_commune": "codgeo"})
#                 .copy()
#             )

#         if {"code_commune", "libelle_commune", "code_departement"}.issubset(df_t2.columns):
#             frames.append(
#                 df_t2[["code_commune", "libelle_commune", "code_departement"]]
#                 .rename(columns={"code_commune": "codgeo"})
#                 .copy()
#             )

#         if "codgeo" in df_emploi.columns:
#             tmp = df_emploi[["codgeo"]].copy()
#             tmp["libelle_commune"] = None
#             tmp["code_departement"] = tmp["codgeo"].astype(str).str[:2]
#             frames.append(tmp)

#         if "codgeo" in df_revenus.columns:
#             tmp = df_revenus[["codgeo"]].copy()
#             tmp["libelle_commune"] = None
#             tmp["code_departement"] = tmp["codgeo"].astype(str).str[:2]
#             frames.append(tmp)

#         if not frames:
#             return pd.DataFrame(columns=["codgeo", "libelle_commune", "code_departement"])

#         dim = pd.concat(frames, ignore_index=True).drop_duplicates()
#         dim["codgeo"] = self._safe_str(dim["codgeo"], 5)
#         dim["code_departement"] = self._safe_str(dim["code_departement"], 2)

#         dim["libelle_commune"] = (
#             dim["libelle_commune"]
#             .astype(str)
#             .replace("None", pd.NA)
#             .str.strip()
#             .str.title()
#         )

#         dim = dim.sort_values(["codgeo", "libelle_commune"], na_position="last")
#         dim = dim.drop_duplicates(subset=["codgeo"], keep="first").reset_index(drop=True)

#         self._log(f"  dim_commune → {len(dim):>6} lignes")
#         return dim

#     def build_dim_departement(
#         self,
#         df_t1: pd.DataFrame,
#         df_t2: pd.DataFrame,
#         df_securite: pd.DataFrame
#     ) -> pd.DataFrame:
#         frames = []

#         if {"code_departement", "libelle_departement"}.issubset(df_t1.columns):
#             tmp = df_t1[["code_departement", "libelle_departement"]].copy()
#             tmp["code_region"] = None
#             frames.append(tmp)

#         if {"code_departement", "libelle_departement"}.issubset(df_t2.columns):
#             tmp = df_t2[["code_departement", "libelle_departement"]].copy()
#             tmp["code_region"] = None
#             frames.append(tmp)

#         if {"code_departement", "code_region"}.issubset(df_securite.columns):
#             tmp = df_securite[["code_departement", "code_region"]].copy()
#             tmp["libelle_departement"] = "Rhone"
#             frames.append(tmp)

#         if not frames:
#             return pd.DataFrame(columns=["code_departement", "libelle_departement", "code_region"])

#         dim = pd.concat(frames, ignore_index=True).drop_duplicates()
#         dim["code_departement"] = self._safe_str(dim["code_departement"], 2)

#         if "code_region" in dim.columns:
#             dim["code_region"] = dim["code_region"].astype(str).replace("None", pd.NA).str.zfill(2)

#         dim["libelle_departement"] = (
#             dim["libelle_departement"]
#             .astype(str)
#             .replace("None", pd.NA)
#             .str.strip()
#             .str.title()
#         )

#         dim = dim.sort_values(["code_departement", "libelle_departement"], na_position="last")
#         dim = dim.drop_duplicates(subset=["code_departement"], keep="first").reset_index(drop=True)

#         self._log(f"  dim_departement → {len(dim):>6} lignes")
#         return dim

#     def build_dim_temps(self) -> pd.DataFrame:
#         dim = pd.DataFrame({
#             "annee": [2021, 2022, 2024],
#             "libelle_annee": ["2021", "2022", "2024"],
#             "periode_source": [
#                 "revenus",
#                 "emploi_diplomes_logement",
#                 "elections_securite"
#             ]
#         })

#         self._log(f"  dim_temps   → {len(dim):>6} lignes")
#         return dim

#     # ────────────────────────────────────────────────────────────────────────
#     # ENRICHISSEMENT DES FACTS
#     # ────────────────────────────────────────────────────────────────────────

#     def add_fact_time_keys(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
#         out = df.copy()
#         out["annee"] = year
#         return out

#     def rename_geo_keys(self, df: pd.DataFrame) -> pd.DataFrame:
#         out = df.copy()

#         if "code_commune" in out.columns and "codgeo" not in out.columns:
#             out = out.rename(columns={"code_commune": "codgeo"})

#         return out

#     # ────────────────────────────────────────────────────────────────────────
#     # LOAD
#     # ────────────────────────────────────────────────────────────────────────

#     def _save_csv(self, df: pd.DataFrame, filename: str) -> None:
#         path = os.path.join(self.out_folder, filename)
#         df.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
#         self._log(f"  CSV    → {filename}  ({len(df)} lignes)")

#     def _save_duckdb(self, df: pd.DataFrame, table: str) -> None:
#         con = duckdb.connect(self.db_path)
#         con.register("_tmp", df)
#         con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _tmp")
#         con.close()
#         self._log(f"  DuckDB → {table}")

#     def load(self, tables: dict) -> None:
#         self._log("\n── LOAD ─────────────────────────────────────────")
#         for table_name, (df, csv_name) in tables.items():
#             self._save_csv(df, csv_name)
#             self._save_duckdb(df, table_name)

#     # ────────────────────────────────────────────────────────────────────────
#     # QUALITY REPORT
#     # ────────────────────────────────────────────────────────────────────────

#     def save_quality_report(self) -> None:
#         if not self.quality_rows:
#             return

#         report = pd.DataFrame(self.quality_rows)
#         filename = "quality_report.csv"
#         path = os.path.join(self.out_folder, filename)
#         report.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
#         self._log(f"\n  Rapport qualité → {filename}")

#     # ────────────────────────────────────────────────────────────────────────
#     # VUES ANALYTIQUES DUCKDB
#     # ────────────────────────────────────────────────────────────────────────

#     def build_views(self) -> None:
#         self._log("\n── VUES ANALYTIQUES ─────────────────────────────")

#         con = duckdb.connect(self.db_path)

#         con.execute("""
#         CREATE OR REPLACE VIEW vw_elections_communes AS
#         SELECT
#             f1.codgeo,
#             dc.libelle_commune,
#             dd.libelle_departement,
#             f1.annee,
#             f1.inscrits,
#             f1.votants,
#             f1.blancs,
#             f1.nuls,
#             f1.exprimes,
#             CASE
#                 WHEN f1.inscrits > 0 THEN ROUND((f1.votants::DOUBLE / f1.inscrits::DOUBLE) * 100, 2)
#                 ELSE NULL
#             END AS taux_participation
#         FROM fact_election_tour1 f1
#         LEFT JOIN dim_commune dc
#             ON f1.codgeo = dc.codgeo
#         LEFT JOIN dim_departement dd
#             ON f1.code_departement = dd.code_departement
#         """)

#         con.execute("""
#         CREATE OR REPLACE VIEW vw_indicateurs_territoires AS
#         SELECT
#             dc.codgeo,
#             dc.libelle_commune,
#             dd.libelle_departement,
#             r.* EXCLUDE (codgeo),
#             e.* EXCLUDE (codgeo)
#         FROM dim_commune dc
#         LEFT JOIN dim_departement dd
#             ON dc.code_departement = dd.code_departement
#         LEFT JOIN fact_revenus r
#             ON dc.codgeo = r.codgeo
#         LEFT JOIN fact_emploi_pop_active e
#             ON dc.codgeo = e.codgeo
#         """)

#         con.close()
#         self._log("  Vues créées : vw_elections_communes, vw_indicateurs_territoires")

#     # ────────────────────────────────────────────────────────────────────────
#     # RUN
#     # ────────────────────────────────────────────────────────────────────────

#     def run(self) -> None:
#         try:
#             raw = self.extract()

#             self._log("\n── TRANSFORM ────────────────────────────────────")

#             clean_t1 = self.clean_election(raw["t1"], "tour1")
#             clean_t2 = self.clean_election(raw["t2"], "tour2")
#             clean_securite = self.clean_securite(raw["securite"])
#             clean_emploi = self.clean_emploi(raw["emploi"])
#             clean_revenus = self.clean_revenus(raw["revenus"])
#             clean_diplomes = self.clean_diplomes(raw["diplomes"])
#             clean_logement = self.clean_logement(raw["logement"])
#             clean_iris = self.clean_iris(raw["iris"])

#             # Harmonisation clés géographiques
#             clean_t1 = self.rename_geo_keys(clean_t1)
#             clean_t2 = self.rename_geo_keys(clean_t2)

#             # Ajout de la clé temps
#             clean_t1 = self.add_fact_time_keys(clean_t1, 2024)
#             clean_t2 = self.add_fact_time_keys(clean_t2, 2024)
#             clean_securite = self.add_fact_time_keys(clean_securite, 2024)
#             clean_emploi = self.add_fact_time_keys(clean_emploi, 2022)
#             clean_revenus = self.add_fact_time_keys(clean_revenus, 2021)
#             clean_diplomes = self.add_fact_time_keys(clean_diplomes, 2022)
#             clean_logement = self.add_fact_time_keys(clean_logement, 2022)

#             # Dimensions
#             dim_commune = self.build_dim_commune(
#                 clean_t1, clean_t2, clean_emploi, clean_revenus
#             )
#             dim_departement = self.build_dim_departement(
#                 clean_t1, clean_t2, clean_securite
#             )
#             dim_temps = self.build_dim_temps()

#             # Tables finales
#             warehouse_tables = {
#                 "fact_election_tour1": (clean_t1, "fact_election_tour1.csv"),
#                 "fact_election_tour2": (clean_t2, "fact_election_tour2.csv"),
#                 "fact_securite": (clean_securite, "fact_securite.csv"),
#                 "fact_emploi_pop_active": (clean_emploi, "fact_emploi_pop_active.csv"),
#                 "fact_revenus": (clean_revenus, "fact_revenus.csv"),
#                 "fact_diplomes": (clean_diplomes, "fact_diplomes.csv"),
#                 "fact_logement": (clean_logement, "fact_logement.csv"),
#                 "dim_iris_lyon": (clean_iris, "dim_iris_lyon.csv"),
#                 "dim_commune": (dim_commune, "dim_commune.csv"),
#                 "dim_departement": (dim_departement, "dim_departement.csv"),
#                 "dim_temps": (dim_temps, "dim_temps.csv"),
#             }

#             self.load(warehouse_tables)
#             self.save_quality_report()
#             self.build_views()

#             self._log("\n✔ ETL terminé avec succès !")
#             self._log(f"   CSV     → {self.out_folder}")
#             self._log(f"   DuckDB  → {self.db_path}")
#             self._log(f"   Log     → {self.log_path}")

#         except Exception as e:
#             logging.exception("Erreur pendant l'exécution ETL")
#             print(f"\n✘ Erreur ETL : {e}")
#             raise


# # ════════════════════════════════════════════════════════════════════════════
# if __name__ == "__main__":
#     etl = ETL()
#     etl.run()

"""
ETL Pipeline — MSPR BLOC 3
Version enrichie avec entrepôt décisionnel simple (DuckDB)

Sources :
  - Premier_tour_rhone_sep.csv        → fact_election_tour1
  - Second_tour_rhone_sep.csv         → fact_election_tour2
  - donnee-dep-...csv                 → fact_securite
  - emploi_et_pop-active_2022.CSV     → fact_emploi_pop_active
  - data_revenus_2021.csv             → fact_revenus
  - diplomes_et_formation-2022.CSV    → fact_diplomes
  - logement-2022.CSV                 → fact_logement
  - contours-iris-metropole-lyon.csv  → dim_iris_lyon
  - candidats_results.csv             → fact_candidats_blocs  ← NOUVEAU

Dimensions construites :
  - dim_commune
  - dim_departement
  - dim_temps

Sorties :
  - CSV nettoyés
  - Tables DuckDB
  - Rapport qualité CSV
  - Vues analytiques SQL
"""

import os
import logging
from datetime import datetime

import unicodedata

import yaml
import pandas as pd
import duckdb


# ════════════════════════════════════════════════════════════════════════════
# ETL
# ════════════════════════════════════════════════════════════════════════════

class ETL:

    # Mapping candidats → blocs politiques (Présidentielle 2022)
    MAPPING_BLOCS = {
        "ARTHAUD":       "Gauche",
        "ROUSSEL":       "Gauche",
        "MÉLENCHON":     "Gauche",
        "MELENCHON":     "Gauche",
        "HIDALGO":       "Gauche",
        "JADOT":         "Gauche",
        "POUTOU":        "Gauche",
        "MACRON":        "Centre",
        "LASSALLE":      "Centre",
        "PÉCRESSE":      "Droite",
        "PECRESSE":      "Droite",
        "LE PEN":        "Extreme_Droite",
        "ZEMMOUR":       "Extreme_Droite",
        "DUPONT-AIGNAN": "Extreme_Droite",
        "DUPONT AIGNAN": "Extreme_Droite",
    }

    def __init__(self, config_path: str = "config.yaml"):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cfg_full = os.path.join(base_dir, config_path)

        if not os.path.exists(cfg_full):
            raise FileNotFoundError(f"config.yaml introuvable : {cfg_full}")

        with open(cfg_full, encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)

        self.base       = self.cfg["data_path"]
        self.files      = self.cfg["files"]
        self.out_folder = self.cfg["output_path"]
        self.db_path    = self.cfg.get("db_path", "warehouse.duckdb")
        self.log_path   = self.cfg.get("log_path", "etl.log")

        os.makedirs(self.out_folder, exist_ok=True)

        self.quality_rows = []
        self._setup_logger()

    # ────────────────────────────────────────────────────────────────────────
    # LOGGER
    # ────────────────────────────────────────────────────────────────────────

    def _setup_logger(self) -> None:
        logging.basicConfig(
            filename=self.log_path,
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            encoding="utf-8"
        )
        logging.info("=== Démarrage ETL ===")

    def _log(self, msg: str) -> None:
        print(msg)
        logging.info(msg)

    # ────────────────────────────────────────────────────────────────────────
    # HELPERS INTERNES
    # ────────────────────────────────────────────────────────────────────────

    def _read(self, filename: str, encoding: str = "utf-8") -> pd.DataFrame:
        path = os.path.join(self.base, filename)

        if not os.path.exists(path):
            raise FileNotFoundError(f"Fichier introuvable : {path}")

        df = pd.read_csv(
            path,
            sep=";",
            encoding=encoding,
            dtype=str,
            low_memory=False
        )
        return df

    def _read_chunked(self, filename: str, encoding: str = "utf-8",
                      chunk_size: int = 100_000) -> pd.DataFrame:
        """
        Lecture par chunks pour les gros fichiers (ex: candidats_results.csv ~2.2 Go).
        Filtre immédiatement sur le département 69 pour limiter la RAM.
        """
        path = os.path.join(self.base, filename)

        if not os.path.exists(path):
            raise FileNotFoundError(f"Fichier introuvable : {path}")

        chunks = []
        for i, chunk in enumerate(pd.read_csv(
            path,
            sep=";",
            encoding=encoding,
            dtype=str,
            low_memory=False,
            chunksize=chunk_size
        )):
            # Filtre Rhône dès la lecture pour économiser la RAM
            if "code_departement" in chunk.columns:
                filtre = chunk[chunk["code_departement"].astype(str).str.strip() == "69"]
            else:
                filtre = chunk[chunk["id_brut_miom"].astype(str).str.startswith("69")]

            if len(filtre) > 0:
                chunks.append(filtre)

            if (i + 1) % 50 == 0:
                self._log(f"    ... {(i + 1) * chunk_size:,} lignes lues")

        if not chunks:
            raise ValueError(f"Aucune donnée Rhône trouvée dans {filename}")

        return pd.concat(chunks, ignore_index=True)

    @staticmethod
    def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = (
            df.columns
            .str.strip()
            .str.lstrip("\ufeff")
            .str.strip('"')
            .str.lower()
            .str.replace(r"[\s\-/]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )
        return df

    @staticmethod
    def _to_num(series: pd.Series) -> pd.Series:
        return pd.to_numeric(
            series.astype(str)
            .str.replace(",", ".", regex=False)
            .str.strip(),
            errors="coerce"
        )

    @staticmethod
    def _is_rhone(code: str) -> bool:
        code = str(code).strip()
        return code.startswith("69") and len(code) >= 5

    @staticmethod
    def _safe_str(series: pd.Series, zfill: int | None = None) -> pd.Series:
        s = series.astype(str).str.strip()
        if zfill is not None:
            s = s.str.zfill(zfill)
        return s

    @staticmethod
    def _repair_mojibake_text(value):
        if pd.isna(value):
            return value

        text = str(value).strip()
        if text == "" or text.lower() == "nan":
            return pd.NA

        if any(ch in text for ch in ("Ã", "Â", "â", "ð", "?")):
            try:
                text = text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
            except Exception:
                pass

        return text

    def _clean_text_series(self, series: pd.Series, remove_accents: bool = False) -> pd.Series:
        s = series.apply(self._repair_mojibake_text)
        s = s.astype("string").str.strip().str.title()

        s = s.replace({
            "Rhone": "Rhône",
            "1Ere": "1ère", "2Eme": "2ème", "3Eme": "3ème",
            "4Eme": "4ème", "5Eme": "5ème", "6Eme": "6ème",
            "7Eme": "7ème", "8Eme": "8ème", "9Eme": "9ème",
        })
        s = s.str.replace(r"(\d+)Ème", r"\1ème", regex=True)

        if remove_accents:
            s = s.apply(
                lambda x: unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode("ascii")
                if pd.notna(x) else x
            )
        return s

    def _add_quality_row(self, table_name, rows_before, rows_after,
                         cols_count, null_cells, duplicates_removed, notes=""):
        self.quality_rows.append({
            "table_name":          table_name,
            "rows_before":         rows_before,
            "rows_after":          rows_after,
            "rows_removed":        rows_before - rows_after,
            "columns_count":       cols_count,
            "null_cells":          null_cells,
            "duplicates_removed":  duplicates_removed,
            "load_timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "notes":               notes
        })

    def _require_cols(self, df: pd.DataFrame, required_cols: list[str], table_name: str) -> None:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"Colonnes manquantes dans {table_name} : {missing}. "
                f"Colonnes présentes : {list(df.columns)}"
            )

    # ────────────────────────────────────────────────────────────────────────
    # EXTRACT
    # ────────────────────────────────────────────────────────────────────────

    def extract(self) -> dict:
        self._log("\n── EXTRACT ──────────────────────────────────────")

        data = {
            "t1":        self._read(self.files["t1"],        encoding="utf-8"),
            "t2":        self._read(self.files["t2"],        encoding="utf-8"),
            "securite":  self._read(self.files["securite"],  encoding="latin1"),
            "emploi":    self._read(self.files["emploi"],    encoding="latin1"),
            "revenus":   self._read(self.files["revenus"],   encoding="latin1"),
            "diplomes":  self._read(self.files["diplomes"],  encoding="latin1"),
            "logement":  self._read(self.files["logement"],  encoding="latin1"),
            "iris":      self._read(self.files["iris"],      encoding="utf-8"),
        }

        # Fichier candidats — lecture par chunks car ~2.2 Go
        self._log("  candidats    → lecture par chunks (fichier volumineux)...")
        data["candidats"] = self._read_chunked(
            self.files["candidats"],
            encoding="utf-8"
        )

        for k, df in data.items():
            self._log(f"  {k:12s} → {len(df):>6} lignes brutes")

        return data

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Élections
    # ────────────────────────────────────────────────────────────────────────

    def clean_election(self, df: pd.DataFrame, label: str) -> pd.DataFrame:
        rows_before = len(df)
        df = self._norm_cols(df)

        self._require_cols(
            df,
            ["code_commune", "code_departement", "inscrits", "votants", "blancs", "nuls", "exprimes"],
            f"election_{label}"
        )

        for col in ["code_canton", "libelle_canton"]:
            if col in df.columns and df[col].isna().all():
                df = df.drop(columns=[col])

        vote_cols  = ["inscrits", "abstentions", "votants", "blancs", "nuls", "exprimes"]
        ratio_cols = [c for c in df.columns if c.startswith("ratio_")]

        for col in vote_cols + ratio_cols:
            if col in df.columns:
                df[col] = self._to_num(df[col])

        df = df.dropna(subset=["inscrits"])
        df = df[df["votants"] <= df["inscrits"]]
        df = df[df["exprimes"] == (df["votants"] - df["blancs"] - df["nuls"])]

        text_cols = ["libelle_commune", "libelle_circonscription",
                     "libelle_departement", "libelle_region"]
        for col in text_cols:
            if col in df.columns:
                df[col] = self._clean_text_series(df[col])
        for col in text_cols:
            if col in df.columns:
                df[col] = self._clean_text_series(df[col], remove_accents=True)

        df["code_commune"]     = self._safe_str(df["code_commune"],     5)
        df["code_departement"] = self._safe_str(df["code_departement"], 2)

        if "code_circonscription" in df.columns:
            df["code_circonscription"] = self._safe_str(df["code_circonscription"])
        if "id_brut_miom" in df.columns:
            df["id_brut_miom"] = df["id_brut_miom"].astype(str).str.strip().str.replace("_", "", regex=False)

        dup_before        = len(df)
        df                = df.drop_duplicates()
        duplicates_removed = dup_before - len(df)

        self._add_quality_row(
            table_name=f"fact_election_{label}",
            rows_before=rows_before, rows_after=len(df),
            cols_count=len(df.columns), null_cells=int(df.isna().sum().sum()),
            duplicates_removed=duplicates_removed,
            notes="Nettoyage élection + contrôle cohérence votants/exprimes"
        )
        self._log(f"  {label:12s} → {len(df):>6} lignes propres")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Candidats / Blocs politiques  ← NOUVEAU
    # ────────────────────────────────────────────────────────────────────────

    def clean_candidats_blocs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie candidats_results.csv et construit fact_candidats_blocs :
        - Filtre sur la Présidentielle 2022 — 1er tour
        - Mappe chaque candidat vers un bloc politique (Gauche / Centre / Droite / Extreme_Droite)
        - Agrège par commune : voix et % par bloc + bloc_gagnant
        - Enrichit avec les taux de participation du 1er tour
        """
        rows_before = len(df)
        df = self._norm_cols(df)

        self._require_cols(df, ["id_election", "id_brut_miom", "voix"], "fact_candidats_blocs")

        # ── Filtre Présidentielle 2022 T1 ────────────────────────────────────
        df = df[df["id_election"].astype(str).str.strip() == "2022_pres_t1"].copy()
        self._log(f"  candidats    → {len(df):>6} lignes après filtre 2022_pres_t1")

        # ── Extraire code_commune depuis id_brut_miom (format: 69XXX_YYYY) ──
        df["code_commune"] = (
            df["id_brut_miom"].astype(str).str.strip().str.split("_").str[0].str.zfill(5)
        )

        # ── Conversion voix en numérique ─────────────────────────────────────
        df["voix"] = self._to_num(df["voix"]).fillna(0).astype(int)

        # ── Suppression des colonnes 100% vides ──────────────────────────────
        cols_vides = [c for c in ["nuance", "liste", "libelle_abrege_liste",
                                   "libelle_etendu_liste", "nom_tete_liste", "binome"]
                      if c in df.columns and df[c].isna().all()]
        if cols_vides:
            df = df.drop(columns=cols_vides)

        # ── Mapping candidat → bloc politique ────────────────────────────────
        col_nom = "nom" if "nom" in df.columns else None
        if col_nom is None:
            raise ValueError("Colonne 'nom' introuvable dans candidats_results.csv")

        df["nom_upper"] = df[col_nom].astype(str).str.upper().str.strip()
        df["bloc"]      = df["nom_upper"].map(self.MAPPING_BLOCS)

        # Vérification des candidats non mappés
        non_mappes = df[df["bloc"].isna()]["nom_upper"].unique()
        if len(non_mappes) > 0:
            self._log(f"  ATTENTION — Candidats non mappés : {list(non_mappes)}")

        # ── Agrégation par commune et par bloc ───────────────────────────────
        df_commune_bloc = (
            df.groupby(["code_commune", "bloc"])["voix"]
            .sum()
            .reset_index()
        )

        # Pivot : une ligne par commune, une colonne par bloc
        df_pivot = (
            df_commune_bloc
            .pivot(index="code_commune", columns="bloc", values="voix")
            .fillna(0)
            .reset_index()
        )
        df_pivot.columns.name = None

        # S'assurer que les 4 blocs sont présents
        for bloc in ["Gauche", "Centre", "Droite", "Extreme_Droite"]:
            if bloc not in df_pivot.columns:
                df_pivot[bloc] = 0

        # ── Variable cible : bloc gagnant ────────────────────────────────────
        blocs = ["Gauche", "Centre", "Droite", "Extreme_Droite"]
        df_pivot["bloc_gagnant"] = df_pivot[blocs].idxmax(axis=1)
        df_pivot["total_voix"]   = df_pivot[blocs].sum(axis=1)

        # Pourcentages par bloc
        for bloc in blocs:
            df_pivot[f"pct_{bloc}"] = (
                df_pivot[bloc] / df_pivot["total_voix"].replace(0, pd.NA) * 100
            ).round(2)

        # ── Nommage cohérent avec le reste de l'entrepôt ────────────────────
        df_pivot = df_pivot.rename(columns={"code_commune": "codgeo"})
        df_pivot["annee"] = 2022

        dup_before         = len(df_pivot)
        df_pivot           = df_pivot.drop_duplicates(subset=["codgeo"])
        duplicates_removed = dup_before - len(df_pivot)

        self._add_quality_row(
            table_name="fact_candidats_blocs",
            rows_before=rows_before, rows_after=len(df_pivot),
            cols_count=len(df_pivot.columns),
            null_cells=int(df_pivot.isna().sum().sum()),
            duplicates_removed=duplicates_removed,
            notes="Filtre 2022_pres_t1 + mapping blocs + agrégation commune + variable cible bloc_gagnant"
        )

        self._log(f"  candidats    → {len(df_pivot):>6} communes propres (fact_candidats_blocs)")
        self._log(f"  Distribution bloc_gagnant :")
        for bloc, cnt in df_pivot["bloc_gagnant"].value_counts().items():
            self._log(f"    {bloc:20s} : {cnt} communes")

        return df_pivot

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Sécurité
    # ────────────────────────────────────────────────────────────────────────

    def clean_securite(self, df: pd.DataFrame) -> pd.DataFrame:
        rows_before = len(df)
        df = self._norm_cols(df)

        dept_col = df.columns[0]
        df = df.rename(columns={dept_col: "code_departement"})

        self._require_cols(df, ["code_departement", "code_region", "annee"], "fact_securite")

        df["code_departement"] = (
            df["code_departement"].astype(str).str.strip().str.strip('"').str.zfill(2)
        )
        df["code_region"] = df["code_region"].astype(str).str.strip().str.zfill(2)
        df = df[df["code_departement"] == "69"].copy()

        for col in ["nombre", "taux_pour_mille", "insee_pop", "insee_log"]:
            if col in df.columns:
                df[col] = self._to_num(df[col])

        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

        dup_before         = len(df)
        df                 = df.drop_duplicates()
        duplicates_removed = dup_before - len(df)

        self._add_quality_row(
            table_name="fact_securite",
            rows_before=rows_before, rows_after=len(df),
            cols_count=len(df.columns), null_cells=int(df.isna().sum().sum()),
            duplicates_removed=duplicates_removed,
            notes="Filtre Rhône + conversion colonnes numériques"
        )
        self._log(f"  securite    → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Emploi
    # ────────────────────────────────────────────────────────────────────────

    def clean_emploi(self, df: pd.DataFrame) -> pd.DataFrame:
        rows_before = len(df)
        df = self._norm_cols(df)

        self._require_cols(df, ["codgeo"], "fact_emploi_pop_active")

        df = df[df["codgeo"].apply(self._is_rhone)].copy()
        df["codgeo"] = self._safe_str(df["codgeo"], 5)

        for col in df.columns:
            if col != "codgeo":
                df[col] = self._to_num(df[col])

        df = df.dropna(how="all", subset=[c for c in df.columns if c != "codgeo"])

        dup_before         = len(df)
        df                 = df.drop_duplicates(subset=["codgeo"])
        duplicates_removed = dup_before - len(df)

        self._add_quality_row(
            table_name="fact_emploi_pop_active",
            rows_before=rows_before, rows_after=len(df),
            cols_count=len(df.columns), null_cells=int(df.isna().sum().sum()),
            duplicates_removed=duplicates_removed,
            notes="Filtre Rhône niveau commune"
        )
        self._log(f"  emploi      → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Revenus
    # ────────────────────────────────────────────────────────────────────────

    def clean_revenus(self, df: pd.DataFrame) -> pd.DataFrame:
        rows_before = len(df)
        df = self._norm_cols(df)

        self._require_cols(df, ["codgeo"], "fact_revenus")

        df = df[df["codgeo"].apply(self._is_rhone)].copy()
        df["codgeo"] = self._safe_str(df["codgeo"], 5)

        for col in df.columns:
            if col != "codgeo":
                df[col] = self._to_num(df[col])

        dup_before         = len(df)
        df                 = df.drop_duplicates(subset=["codgeo"])
        duplicates_removed = dup_before - len(df)

        self._add_quality_row(
            table_name="fact_revenus",
            rows_before=rows_before, rows_after=len(df),
            cols_count=len(df.columns), null_cells=int(df.isna().sum().sum()),
            duplicates_removed=duplicates_removed,
            notes="Filtre Rhône niveau commune/IRIS selon source"
        )
        self._log(f"  revenus     → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Diplômes
    # ────────────────────────────────────────────────────────────────────────

    def clean_diplomes(self, df: pd.DataFrame) -> pd.DataFrame:
        rows_before = len(df)
        df = self._norm_cols(df)
        df = df.rename(columns={"iris": "codeiris", "com": "codgeo"})

        df.columns = [c.encode("ascii", "ignore").decode() for c in df.columns]
        df.columns = pd.Index([c.strip("_") for c in df.columns])

        self._require_cols(df, ["codeiris", "codgeo"], "fact_diplomes")

        df["codgeo"] = df["codgeo"].astype(str).str.strip()
        df = df[df["codgeo"].apply(self._is_rhone)].copy()
        df["codgeo"]   = self._safe_str(df["codgeo"], 5)
        df["codeiris"] = self._safe_str(df["codeiris"])

        key_cols = {"codeiris", "codgeo", "typ_iris", "lab_iris"}
        for col in df.columns:
            if col not in key_cols:
                df[col] = self._to_num(df[col])

        num_cols = [c for c in df.columns if c not in key_cols]
        df = df.dropna(how="all", subset=num_cols)

        dup_before         = len(df)
        df                 = df.drop_duplicates(subset=["codeiris"])
        duplicates_removed = dup_before - len(df)

        self._add_quality_row(
            table_name="fact_diplomes",
            rows_before=rows_before, rows_after=len(df),
            cols_count=len(df.columns), null_cells=int(df.isna().sum().sum()),
            duplicates_removed=duplicates_removed,
            notes="Filtre Rhône niveau IRIS"
        )
        self._log(f"  diplomes    → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — Logement
    # ────────────────────────────────────────────────────────────────────────

    def clean_logement(self, df: pd.DataFrame) -> pd.DataFrame:
        rows_before = len(df)
        df = self._norm_cols(df)
        df = df.rename(columns={"iris": "codeiris", "com": "codgeo"})

        self._require_cols(df, ["codeiris", "codgeo"], "fact_logement")

        df["codgeo"] = df["codgeo"].astype(str).str.strip()
        df = df[df["codgeo"].apply(self._is_rhone)].copy()
        df["codgeo"]   = self._safe_str(df["codgeo"], 5)
        df["codeiris"] = self._safe_str(df["codeiris"])

        key_cols = {"codeiris", "codgeo", "typ_iris", "lab_iris"}
        for col in df.columns:
            if col not in key_cols:
                df[col] = self._to_num(df[col])

        num_cols = [c for c in df.columns if c not in key_cols]
        df = df.dropna(how="all", subset=num_cols)

        dup_before         = len(df)
        df                 = df.drop_duplicates(subset=["codeiris"])
        duplicates_removed = dup_before - len(df)

        self._add_quality_row(
            table_name="fact_logement",
            rows_before=rows_before, rows_after=len(df),
            cols_count=len(df.columns), null_cells=int(df.isna().sum().sum()),
            duplicates_removed=duplicates_removed,
            notes="Filtre Rhône niveau IRIS"
        )
        self._log(f"  logement    → {len(df):>6} lignes propres (Rhône)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # TRANSFORM — IRIS Lyon
    # ────────────────────────────────────────────────────────────────────────

    def clean_iris(self, df: pd.DataFrame) -> pd.DataFrame:
        rows_before = len(df)
        df = self._norm_cols(df)

        self._require_cols(df, ["codeiris"], "dim_iris_lyon")

        df["codeiris"] = df["codeiris"].astype(str).str.strip()

        if "insee" in df.columns:
            df["insee"] = df["insee"].astype(str).str.strip()
        if "gid" in df.columns:
            df["gid"] = pd.to_numeric(df["gid"], errors="coerce")

        for col in ["libelle", "commune", "typelibelle"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()

        if "type" in df.columns:
            df["type"] = df["type"].astype(str).str.strip().str.upper()

        dup_before         = len(df)
        df                 = df.drop_duplicates(subset=["codeiris"])
        duplicates_removed = dup_before - len(df)

        self._add_quality_row(
            table_name="dim_iris_lyon",
            rows_before=rows_before, rows_after=len(df),
            cols_count=len(df.columns), null_cells=int(df.isna().sum().sum()),
            duplicates_removed=duplicates_removed,
            notes="Nettoyage référentiel IRIS Lyon"
        )
        self._log(f"  iris        → {len(df):>6} lignes propres (métropole Lyon)")
        return df

    # ────────────────────────────────────────────────────────────────────────
    # BUILD DIMENSIONS
    # ────────────────────────────────────────────────────────────────────────

    def build_dim_commune(self, df_t1, df_t2, df_emploi, df_revenus):
        frames = []

        for df in [df_t1, df_t2]:
            if {"code_commune", "libelle_commune", "code_departement"}.issubset(df.columns):
                frames.append(
                    df[["code_commune", "libelle_commune", "code_departement"]]
                    .rename(columns={"code_commune": "codgeo"})
                    .copy()
                )

        for df in [df_emploi, df_revenus]:
            if "codgeo" in df.columns:
                tmp = df[["codgeo"]].copy()
                tmp["libelle_commune"]  = None
                tmp["code_departement"] = tmp["codgeo"].astype(str).str[:2]
                frames.append(tmp)

        if not frames:
            return pd.DataFrame(columns=["codgeo", "libelle_commune", "code_departement"])

        dim = pd.concat(frames, ignore_index=True).drop_duplicates()
        dim["codgeo"]           = self._safe_str(dim["codgeo"], 5)
        dim["code_departement"] = self._safe_str(dim["code_departement"], 2)
        dim["libelle_commune"]  = (
            dim["libelle_commune"].astype(str).replace("None", pd.NA).str.strip().str.title()
        )

        dim = dim.sort_values(["codgeo", "libelle_commune"], na_position="last")
        dim = dim.drop_duplicates(subset=["codgeo"], keep="first").reset_index(drop=True)

        self._log(f"  dim_commune → {len(dim):>6} lignes")
        return dim

    def build_dim_departement(self, df_t1, df_t2, df_securite):
        frames = []

        for df in [df_t1, df_t2]:
            if {"code_departement", "libelle_departement"}.issubset(df.columns):
                tmp = df[["code_departement", "libelle_departement"]].copy()
                tmp["code_region"] = None
                frames.append(tmp)

        if {"code_departement", "code_region"}.issubset(df_securite.columns):
            tmp = df_securite[["code_departement", "code_region"]].copy()
            tmp["libelle_departement"] = "Rhone"
            frames.append(tmp)

        if not frames:
            return pd.DataFrame(columns=["code_departement", "libelle_departement", "code_region"])

        dim = pd.concat(frames, ignore_index=True).drop_duplicates()
        dim["code_departement"] = self._safe_str(dim["code_departement"], 2)

        if "code_region" in dim.columns:
            dim["code_region"] = dim["code_region"].astype(str).replace("None", pd.NA).str.zfill(2)

        dim["libelle_departement"] = (
            dim["libelle_departement"].astype(str).replace("None", pd.NA).str.strip().str.title()
        )

        dim = dim.sort_values(["code_departement", "libelle_departement"], na_position="last")
        dim = dim.drop_duplicates(subset=["code_departement"], keep="first").reset_index(drop=True)

        self._log(f"  dim_departement → {len(dim):>6} lignes")
        return dim

    def build_dim_temps(self) -> pd.DataFrame:
        dim = pd.DataFrame({
            "annee":          [2021, 2022, 2024],
            "libelle_annee":  ["2021", "2022", "2024"],
            "periode_source": ["revenus", "emploi_diplomes_logement", "elections_securite"]
        })
        self._log(f"  dim_temps   → {len(dim):>6} lignes")
        return dim

    # ────────────────────────────────────────────────────────────────────────
    # ENRICHISSEMENT
    # ────────────────────────────────────────────────────────────────────────

    def add_fact_time_keys(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        out = df.copy()
        out["annee"] = year
        return out

    def rename_geo_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "code_commune" in out.columns and "codgeo" not in out.columns:
            out = out.rename(columns={"code_commune": "codgeo"})
        return out

    # ────────────────────────────────────────────────────────────────────────
    # LOAD
    # ────────────────────────────────────────────────────────────────────────

    def _save_csv(self, df: pd.DataFrame, filename: str) -> None:
        path = os.path.join(self.out_folder, filename)
        df.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
        self._log(f"  CSV    → {filename}  ({len(df)} lignes)")

    def _save_duckdb(self, df: pd.DataFrame, table: str) -> None:
        con = duckdb.connect(self.db_path)
        con.register("_tmp", df)
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _tmp")
        con.close()
        self._log(f"  DuckDB → {table}")

    def load(self, tables: dict) -> None:
        self._log("\n── LOAD ─────────────────────────────────────────")
        for table_name, (df, csv_name) in tables.items():
            self._save_csv(df, csv_name)
            self._save_duckdb(df, table_name)

    # ────────────────────────────────────────────────────────────────────────
    # QUALITY REPORT
    # ────────────────────────────────────────────────────────────────────────

    def save_quality_report(self) -> None:
        if not self.quality_rows:
            return
        report   = pd.DataFrame(self.quality_rows)
        filename = "quality_report.csv"
        path     = os.path.join(self.out_folder, filename)
        report.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
        self._log(f"\n  Rapport qualité → {filename}")

    # ────────────────────────────────────────────────────────────────────────
    # VUES ANALYTIQUES DUCKDB
    # ────────────────────────────────────────────────────────────────────────

    def build_views(self) -> None:
        self._log("\n── VUES ANALYTIQUES ─────────────────────────────")
        con = duckdb.connect(self.db_path)

        con.execute("""
        CREATE OR REPLACE VIEW vw_elections_communes AS
        SELECT
            f1.codgeo,
            dc.libelle_commune,
            dd.libelle_departement,
            f1.annee,
            f1.inscrits,
            f1.votants,
            f1.blancs,
            f1.nuls,
            f1.exprimes,
            CASE
                WHEN f1.inscrits > 0
                THEN ROUND((f1.votants::DOUBLE / f1.inscrits::DOUBLE) * 100, 2)
                ELSE NULL
            END AS taux_participation
        FROM fact_election_tour1 f1
        LEFT JOIN dim_commune     dc ON f1.codgeo           = dc.codgeo
        LEFT JOIN dim_departement dd ON f1.code_departement = dd.code_departement
        """)

        con.execute("""
        CREATE OR REPLACE VIEW vw_indicateurs_territoires AS
        SELECT
            dc.codgeo,
            dc.libelle_commune,
            dd.libelle_departement,
            r.* EXCLUDE (codgeo),
            e.* EXCLUDE (codgeo)
        FROM dim_commune dc
        LEFT JOIN dim_departement        dd ON dc.code_departement = dd.code_departement
        LEFT JOIN fact_revenus           r  ON dc.codgeo           = r.codgeo
        LEFT JOIN fact_emploi_pop_active e  ON dc.codgeo           = e.codgeo
        """)

        # Vue enrichie avec les blocs politiques ← NOUVEAU
        con.execute("""
        CREATE OR REPLACE VIEW vw_elections_blocs AS
        SELECT
            cb.codgeo,
            dc.libelle_commune,
            cb.bloc_gagnant,
            cb.pct_Gauche,
            cb.pct_Centre,
            cb.pct_Droite,
            cb.pct_Extreme_Droite,
            cb.total_voix,
            CASE
                WHEN f1.inscrits > 0
                THEN ROUND((f1.votants::DOUBLE / f1.inscrits::DOUBLE) * 100, 2)
                ELSE NULL
            END AS taux_participation_t1
        FROM fact_candidats_blocs cb
        LEFT JOIN dim_commune dc
            ON cb.codgeo = dc.codgeo
        LEFT JOIN (
            SELECT codgeo,
                   SUM(inscrits) AS inscrits,
                   SUM(votants)  AS votants
            FROM fact_election_tour1
            GROUP BY codgeo
        ) f1 ON cb.codgeo = f1.codgeo
        """)

        con.close()
        self._log("  Vues créées : vw_elections_communes, vw_indicateurs_territoires, vw_elections_blocs")

    # ────────────────────────────────────────────────────────────────────────
    # RUN
    # ────────────────────────────────────────────────────────────────────────

    def run(self) -> None:
        try:
            raw = self.extract()

            self._log("\n── TRANSFORM ────────────────────────────────────")

            clean_t1        = self.clean_election(raw["t1"],       "tour1")
            clean_t2        = self.clean_election(raw["t2"],       "tour2")
            clean_securite  = self.clean_securite(raw["securite"])
            clean_emploi    = self.clean_emploi(raw["emploi"])
            clean_revenus   = self.clean_revenus(raw["revenus"])
            clean_diplomes  = self.clean_diplomes(raw["diplomes"])
            clean_logement  = self.clean_logement(raw["logement"])
            clean_iris      = self.clean_iris(raw["iris"])
            clean_candidats = self.clean_candidats_blocs(raw["candidats"])  # ← NOUVEAU

            # Harmonisation clés géographiques
            clean_t1 = self.rename_geo_keys(clean_t1)
            clean_t2 = self.rename_geo_keys(clean_t2)

            # Ajout clé temporelle
            clean_t1        = self.add_fact_time_keys(clean_t1,       2024)
            clean_t2        = self.add_fact_time_keys(clean_t2,       2024)
            clean_securite  = self.add_fact_time_keys(clean_securite, 2024)
            clean_emploi    = self.add_fact_time_keys(clean_emploi,   2022)
            clean_revenus   = self.add_fact_time_keys(clean_revenus,  2021)
            clean_diplomes  = self.add_fact_time_keys(clean_diplomes, 2022)
            clean_logement  = self.add_fact_time_keys(clean_logement, 2022)
            # clean_candidats a déjà annee=2022 positionné dans clean_candidats_blocs

            # Dimensions
            dim_commune      = self.build_dim_commune(clean_t1, clean_t2, clean_emploi, clean_revenus)
            dim_departement  = self.build_dim_departement(clean_t1, clean_t2, clean_securite)
            dim_temps        = self.build_dim_temps()

            # Tables finales
            warehouse_tables = {
                "fact_election_tour1":    (clean_t1,        "fact_election_tour1.csv"),
                "fact_election_tour2":    (clean_t2,        "fact_election_tour2.csv"),
                "fact_securite":          (clean_securite,  "fact_securite.csv"),
                "fact_emploi_pop_active": (clean_emploi,    "fact_emploi_pop_active.csv"),
                "fact_revenus":           (clean_revenus,   "fact_revenus.csv"),
                "fact_diplomes":          (clean_diplomes,  "fact_diplomes.csv"),
                "fact_logement":          (clean_logement,  "fact_logement.csv"),
                "fact_candidats_blocs":   (clean_candidats, "fact_candidats_blocs.csv"),  # ← NOUVEAU
                "dim_iris_lyon":          (clean_iris,      "dim_iris_lyon.csv"),
                "dim_commune":            (dim_commune,     "dim_commune.csv"),
                "dim_departement":        (dim_departement, "dim_departement.csv"),
                "dim_temps":              (dim_temps,       "dim_temps.csv"),
            }

            self.load(warehouse_tables)
            self.save_quality_report()
            self.build_views()

            self._log("\n✔ ETL terminé avec succès !")
            self._log(f"   CSV     → {self.out_folder}")
            self._log(f"   DuckDB  → {self.db_path}")
            self._log(f"   Log     → {self.log_path}")

        except Exception as e:
            logging.exception("Erreur pendant l'exécution ETL")
            print(f"\n✘ Erreur ETL : {e}")
            raise


# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    etl = ETL()
    etl.run()