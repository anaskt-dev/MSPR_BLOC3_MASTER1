# MSPR BLOC 3 — ETL Pipeline

Guide d'installation et de lancement du pipeline ETL pour le projet MSPR Bloc 3.

---

## 1. Prérequis

- **Python 3.10 ou supérieur** installé sur la machine
- Vérifier la version :
  ```powershell
  python --version
  ```

---

## 2. Créer un environnement virtuel

> Un environnement virtuel isole les dépendances du projet de celles installées
> sur le PC. Cela évite les conflits entre projets.

Ouvre un terminal PowerShell dans le dossier racine du projet :

```powershell
cd D:\MSPR_CODE\MSPR_BLOC3_MASTER1
```

Crée l'environnement virtuel dans un dossier `.venv` :

```powershell
python -m venv .venv
```

---

## 3. Activer l'environnement virtuel

À chaque fois que tu ouvres un nouveau terminal, tu dois activer l'environnement :

```powershell
.venv\Scripts\activate
```

Tu verras `(.venv)` apparaître au début de la ligne dans ton terminal :

```
(.venv) PS D:\MSPR_CODE\MSPR_BLOC3_MASTER1>
```

Pour désactiver l'environnement en fin de session :

```powershell
deactivate
```

---

## 4. Installer les dépendances

Une fois l'environnement activé, installe les packages **uniquement dans l'environnement** (pas sur le PC) :

```powershell
pip install -r requirements.txt
```

Vérifier que l'installation s'est bien passée :

```powershell
pip list
```

Tu dois voir `pandas`, `PyYAML` et `duckdb` dans la liste.

---

## 5. Configurer les chemins

Ouvre le fichier `etl\config.yaml` et vérifie que les chemins correspondent à ton installation :

```yaml
data_path:   "D:/MSPR_CODE/MSPR_BLOC3_MASTER1/DATA"
output_path: "D:/MSPR_CODE/MSPR_BLOC3_MASTER1/data_clean"
db_path:     "D:/MSPR_CODE/MSPR_BLOC3_MASTER1/warehouse.duckdb"

files:
  t1:       "Premier_tour_rhone_sep.csv"
  t2:       "Second_tour_rhone_sep.csv"
  securite: "donnee-dep-data.gouv-2025-geographie2025-produit-le2026-01-22.csv"
  emploi:   "emploi_et_pop-active_2022.CSV"
  revenus:  "data_revenus_2021.csv"
  diplomes: "diplomes_et_formation-2022.CSV"
  logement: "logement-2022.CSV"
  iris:     "contours-iris-metropole-lyon.csv"
```

---

## 6. Lancer l'ETL

Depuis la racine du projet avec l'environnement activé :

```powershell
python etl\etl_pipeline.py
```

Tu dois voir s'afficher dans le terminal :

```
── EXTRACT ──────────────────────────────────────
  t1           →   1317 lignes brutes
  t2           →   1317 lignes brutes
  securite     →  18180 lignes brutes
  emploi       →  34903 lignes brutes
  revenus      →    777 lignes brutes
  diplomes     →  49276 lignes brutes
  logement     →  49276 lignes brutes
  iris         →    512 lignes brutes

── TRANSFORM ────────────────────────────────────
  tour1        →   1317 lignes propres
  tour2        →   1317 lignes propres
  securite     →    180 lignes propres (Rhône)
  emploi       →    275 lignes propres (Rhône)
  revenus      →     23 lignes propres (Rhône)
  diplomes     →    752 lignes propres (Rhône)
  logement     →    752 lignes propres (Rhône)
  iris         →    512 lignes propres (métropole Lyon)

── LOAD ─────────────────────────────────────────
  CSV  → clean_election_tour1.csv  (1317 lignes)
  ...

✔  ETL terminé avec succès !
```

---

## 7. Résultats générés

Après l'exécution, deux sorties sont créées :

**Fichiers CSV nettoyés** dans `data_clean/` :

| Fichier | Description |
|---|---|
| `clean_election_tour1.csv` | Résultats 1er tour présidentielle 2022 |
| `clean_election_tour2.csv` | Résultats 2ème tour présidentielle 2022 |
| `clean_securite.csv` | Données criminalité Rhône 2016–2025 |
| `clean_emploi_pop_active.csv` | Population active par commune (Rhône) |
| `clean_revenus.csv` | Revenus médians par commune (Rhône) |
| `clean_diplomes.csv` | Niveaux de diplôme par IRIS (Rhône) |
| `clean_logement.csv` | Données logement par IRIS (Rhône) |
| `clean_iris_lyon.csv` | Référentiel IRIS métropole de Lyon |

**Base DuckDB** : `warehouse.duckdb` avec les mêmes données en tables SQL.

---
## 8. Utiliser DuckDB
 
DuckDB est la base de données dans laquelle l'ETL charge toutes les tables nettoyées.
Le fichier `warehouse.duckdb` est créé automatiquement à la racine du projet après le premier lancement de l'ETL.
