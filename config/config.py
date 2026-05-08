config = {
    "project": {
        "name": "HD_quality_water_project_DLT_databricks",
        "owner": "hdjelti.ext@simplonformations.co",
        "version": "4.0.0"
    },

    "ingestion": {
        "annees": [2023, 2024, 2025],
        "departement": "35",

        "api": {
            "base_url": "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable",

            "endpoints": {
                    "analyses": "resultats_dis",
                    "prelevements": "prelevements",
                    "stations": "prelevements",
                    "parametres": "parametres",
                    "communes": "communes_udi"
            },

            "pagination": {
                "enabled": True,
                "page_size": 5000
            },

            "timeout_seconds": 30
        }
    },

    "storage": {
        "mode": "azure",

        "local": {
            "base_path": "./data",
            "bronze_path": "./data/bronze",
            "silver_path": "./data/silver",
            "gold_path": "./data/gold",
            "logs_path": "./data/logs"
        },

        "azure": {
            "account_name": "stqualitywaterdlt",
            "container": "quality-water",
            "mount_point": "/mnt/quality-water"
        }
    },

    "tables": {
        "bronze_stations": "bronze_stations",
        "bronze_analyses": "bronze_analyses",
        "bronze_parametres": "bronze_parametres",

        "silver_stations": "silver_stations",
        "silver_analyses": "silver_analyses",
        "silver_parametres": "silver_parametres",

        "gold_commune_parametre": "gold_qualite_commune_parametre",
        "gold_kpi_conformite_dept": "gold_kpi_conformite_departement",
        "gold_kpi_tendance_parametre": "gold_kpi_tendance_parametre",
        "gold_kpi_top_communes": "gold_kpi_top_communes",
        "gold_kpi_top10_non_conformes": "gold_kpi_top10_non_conformes",
        "gold_kpi_top10_parametres_problemes": "gold_kpi_top10_parametres_problemes",
        "gold_kpi_carte_regionale": "gold_kpi_carte_regionale"
    },

    "quality": {
        "expectations": {
            "enabled": True,
            "fail_on_error": True,
            "save_reports": True,
            "reports_path": "dbfs:/mnt/quality-water/logs/expectations"
        }
    },

    "workflow": {
        "schedule": {
            "cron": "0 2 * * *",
            "timezone": "Europe/Paris"
        },

        "tasks": [
            {"name": "01_ingestion_bronze", "notebook": "Pipeline/notebooks/bronze/01_DLT_Ingestion_Qualite_Eau"},
            {"name": "02_transformation_silver", "notebook": "Pipeline/notebooks/silver/02_Silver_Transformation"},
            {"name": "03_aggregations_gold", "notebook": "Pipeline/notebooks/gold/03_Gold_Agregations"},
            {"name": "04_quality_checks", "notebook": "Pipeline/notebooks/quality/04_Quality_Checks"}
        ]
    },

    "ci_cd": {
        "semantic_release": True,
        "versioning": "semantic",
        "python_tests": True,
        "linting": True,
        "validate_yaml": True
    }
}
