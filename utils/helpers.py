import requests

def call_hubeau(endpoint, params):
    """
    Appelle l'API Hub'Eau et renvoie la liste des données.
    """
    base_url = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable"
    url = f"{base_url}/{endpoint}"
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("data", [])
