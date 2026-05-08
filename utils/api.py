import requests
import time
from typing import Dict, List, Any


class HubeauAPIClient:
    """
    Client générique pour l'API Hub'Eau - Qualité de l'eau potable.
    Gère :
      - pagination
      - timeout
      - endpoints stations / analyses / parametres
    """

    def __init__(self, config: Dict[str, Any]):
        api_cfg = config["ingestion"]["api"]

        self.base_url = api_cfg["base_url"]
        self.endpoints = api_cfg["endpoints"]
        self.page_size = api_cfg["pagination"]["page_size"]
        self.timeout = api_cfg["timeout_seconds"]

    # ---------------------------------------------------------
    # Fonction générique d'appel API avec pagination
    # ---------------------------------------------------------
    def fetch_all(self, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Récupère toutes les pages d'un endpoint Hub'Eau.
        """
        url = f"{self.base_url}/{endpoint}"
        results = []
        page = 1

        while True:
            paginated_params = {
                **params,
                "size": self.page_size,
                "page": page
            }

            try:
                response = requests.get(url, params=paginated_params, timeout=self.timeout)
                response.raise_for_status()
            except Exception as e:
                print(f"❌ Erreur API Hub'Eau ({endpoint}) page {page}: {e}")
                break

            data = response.json()

            if "data" not in data or len(data["data"]) == 0:
                break

            results.extend(data["data"])

            # Pagination Hub'Eau : si moins que page_size → fin
            if len(data["data"]) < self.page_size:
                break

            page += 1
            time.sleep(0.2)  # éviter de spammer l'API

        print(f"✔️ {len(results)} enregistrements récupérés depuis {endpoint}")
        return results

    # ---------------------------------------------------------
    # Fonctions spécialisées
    # ---------------------------------------------------------
    def fetch_stations(self, departement: str) -> List[Dict[str, Any]]:
        return self.fetch_all(
            endpoint=self.endpoints["stations"],
            params={"code_departement": departement}
        )

    def fetch_analyses(self, departement: str, annees: List[int]) -> List[Dict[str, Any]]:
        return self.fetch_all(
            endpoint=self.endpoints["analyses"],
            params={
                "code_departement": departement,
                "annee": ",".join(map(str, annees))
            }
        )

    def fetch_parametres(self) -> List[Dict[str, Any]]:
        return self.fetch_all(
            endpoint=self.endpoints["parametres"],
            params={}
        )
