import pandas as pd
import yaml
import os
import duckdb
# from sqlalchemy import create_engine


class ETL:
    def __init__(self, config_path="config.yaml"):
        self.cfg = yaml.safe_load(open(config_path))
        self.base = self.cfg["data_path"]
        self.files = self.cfg["files"]
        # self.engine = create_engine(self.cfg["db_url"])
        
        # ajouter temporerement les dossiers pour export
        self.out_folder = self.cfg["output_path"]
        os.makedirs(self.out_folder, exist_ok=True)



    # -----------------------------------
    # EXTRACT
    # -----------------------------------
    def load_csv(self, name):
        return pd.read_csv(os.path.join(self.base, name), sep=";")

    def extract_all(self):
        df_t1 = self.load_csv(self.files["t1"])
        df_t2 = self.load_csv(self.files["t2"])
        df_chom = self.load_csv(self.files["chomage"])
        df_sec = self.load_csv(self.files["securite"])
        return df_t1, df_t2, df_chom, df_sec

    # --------------------------------
    # TRANSFORM
    # -----------------------------------
    def normalize_departement(self, code):
        code = str(code).strip().upper().replace(" ", "")

        if code in ["2A", "2B"]:
            return code

        if code.isdigit():
            return code.zfill(2)

        return code
    
    def clean_election(self, df):
        df = df.copy()

        # 1. Normalisation colonnes
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
        )

        for col in ["code_canton", "libelle_canton"]:
            if col in df.columns:
                if df[col].isna().all():
                    df = df.drop(columns=[col])

        # 2. Conversion numérique
        numeric_cols = ["inscrits", "abstentions", "votants", "blancs", "nuls", "exprimes"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 3. Drop lignes vides
        df = df.dropna(subset=["inscrits"])

        # 4. Vérifications d'intégrité
        df = df[df["votants"] <= df["inscrits"]]
        df = df[df["exprimes"] == (df["votants"] - df["blancs"] - df["nuls"])]

        # 5. Suppression doublons
        df = df.drop_duplicates()

        # 6. Nettoyage texte
        text_cols = ["libelle_commune", "libelle_circonscription", 
                    "libelle_canton", "libelle_departement"]
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()

        # # 7. Colonnes taux
        # df["taux_participation"] = df["votants"] / df["inscrits"]
        # df["taux_abstention"] = df["abstentions"] / df["inscrits"]
        # df["taux_blancs_sur_votants"] = df["blancs"] / df["votants"]
        # df["taux_nuls_sur_votants"] = df["nuls"] / df["votants"]
        # df["taux_exprimes_sur_votants"] = df["exprimes"] / df["votants"]

        return df

    def clean_chomage_xls_like(self, df):
        # Renommer les colonnes à partir de la première ligne
        df.columns = ["code_departement", "departement", "taux_chomage"]

        # Enlever la première ligne (elle servait de header)
        df = df[1:]

        # Nettoyage
        df["code_departement"] = df["code_departement"].apply(self.normalize_departement)
        df["departement"] = df["departement"].astype(str).str.strip().str.title()

        # Taux -> convertir virgule en point
        df["taux_chomage"] = (
            df["taux_chomage"]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

        return df

    def clean_securite(self, df): 
        df = df.copy()

        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        df["code_departement"] = df["code_departement"].apply(self.normalize_departement)
        df["code_region"] = df["code_region"].astype(str).str.zfill(2)

        df["nombre"] = pd.to_numeric(df["nombre"], errors="coerce")

        df["taux_pour_mille"] = (
            df["taux_pour_mille"]
            .astype(str)
            .str.replace(",", ".", regex=False)
        )
        df["taux_pour_mille"] = pd.to_numeric(df["taux_pour_mille"], errors="coerce")

        df["insee_pop"] = pd.to_numeric(df["insee_pop"], errors="coerce")
        df["insee_log"] = pd.to_numeric(df["insee_log"], errors="coerce")

        return df

    # -----------------------------------
    # LOAD
    # -----------------------------------
    # def load_to_db(self, **tables):
    #     for name, df in tables.items():
    #         print(f"➡ Loading table '{name}' ...")
    #         df.to_sql(name, self.engine, if_exists="replace", index=False)
    #     print("✔ All tables loaded successfully")
    def load(self, df, filename):

        out_path = os.path.join(self.out_folder, filename)

        df.to_csv(
            out_path,
            sep=";",
            index=False,
            encoding="utf-8-sig"   # important pour Excel
        )

        print("Fichier sauvegardé :", out_path)
        print("Nombre de lignes :", len(df))

    def load_to_duckdb(self, df, table_name):

        con = duckdb.connect("warehouse.duckdb")   # fichier DW
        con.register("tmp_df", df)                 # créer une table temporaire en mémoire
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df")
        con.close()
        print(f"Table '{table_name}' chargée dans DuckDB (warehouse.duckdb)")

    # -----------------------------------
    # RUN ETL COMPLET
    # -----------------------------------
    def run(self):

        print("Extracting...")
        df_t1, df_t2, df_chom, df_sec = self.extract_all()

        print("Transforming...")
        df_t1 = self.clean_election(df_t1)
        df_t2 = self.clean_election(df_t2)
        df_chom = self.clean_chomage_xls_like(df_chom)
        df_sec = self.clean_securite(df_sec)
        # dim_dep = self.build_dim_departement(df_chom)

        print("Loading to SQL...")
        self.load(df_t1, "clean_tour1.csv")
        self.load(df_t2, "clean_tour2.csv")
        self.load(df_chom, "clean_chomage.csv")
        self.load(df_sec, "clean_securite.csv")

        
        print("Loading into DuckDB warehouse...")
        self.load_to_duckdb(df_t1, "fact_election_tour1")
        self.load_to_duckdb(df_t2, "fact_election_tour2")
        self.load_to_duckdb(df_chom, "fact_chomage")
        self.load_to_duckdb(df_sec, "fact_securite")

        # self.load_to_db(
        #     fait_election_t1=df_t1,
        #     fait_election_t2=df_t2,
        #     fait_chomage=df_chom,
        #     fait_securite=df_sec,
        #     dim_departement=dim_dep
        # )

        # print("✔ ETL terminé avec succès !")

if __name__ == "__main__":
    etl = ETL()
    etl.run()