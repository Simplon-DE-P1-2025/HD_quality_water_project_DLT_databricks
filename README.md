# HD_quality_water_project_DLT_databricks
Pipeline Delta Live Tables Databricks  pour l’ingestion, le nettoyage et l’analyse des données de qualité de l’eau issues des API Hub’Eau (architecture Médaillon Bronze/Silver/Gold visualisations analytiques).).
Parfait — on va verrouiller **trois choses essentielles** pour un projet Data Engineering propre et crédible :


##  Objectifs
- Ingestion des données Hub’Eau (Bronze)
- Nettoyage et standardisation (Silver)
- Agrégations et indicateurs (Gold)
- Visualisation des résultats
- Stockage dans ADLS Gen2
- Configuration centralisée via `config.yaml`

---

##  Structure du projet

```
config.yaml
utils/
  api.py
  helpers.py
notebooks/
  bronze/
  silver/
  gold/
  visualisation/
```

---

##  Technologies
- Databricks
- Delta Live Tables
- PySpark
- ADLS Gen2
- YAML configuration
- GitHub versionning

---

##  Pipelines
- **Bronze** : ingestion API Hub’Eau
- **Silver** : nettoyage, cast, normalisation
- **Gold** : indicateurs par commune et paramètre

---

## 📦 Configuration
Tous les paramètres sont centralisés dans :

config.yaml


---

## 🗂️ Stockage Azure
- Storage Account : ADLS Gen2
- Container : `quality-water`
- Monté dans Databricks via `mount_point`

---

##  Visualisation
Un notebook dédié permet d’explorer les indicateurs Gold.

---

##  CI/CD (préparation)
- Lint Python
- Validation YAML
- Structure du repo
- Versionning Git (Conventional Commits)

---

##  Auteurs
HFD DLT_Projet Simplon - Brief 6 - Data Engineering 2026.
