'''Test pour vérifier que config.yaml est valide
'''
import yaml

def test_config_yaml_valid():
    with open("config/config.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    # Vérifie que les sections principales existent
    assert "ingestion" in cfg
    assert "storage" in cfg
    assert "tables" in cfg

    # Vérifie que les années sont une liste
    assert isinstance(cfg["ingestion"]["annees"], list)
