"""
    Appelle l'API Hub'Eau et renvoie la liste des données.
    """
import requests

def call_hubeau(endpoint, params, base_url):
    url = f"{base_url}/{endpoint}"
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("data", [])
