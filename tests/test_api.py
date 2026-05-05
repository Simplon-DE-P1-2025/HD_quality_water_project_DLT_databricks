'''Test pour utils/api.py

Ce test vérifie que :

    call_hubeau() ne plante pas

    renvoie bien une liste

    accepte les paramètres'''


from utils.api import call_hubeau

def test_api_returns_list():
    result = call_hubeau("analyses", {"size": 1})
    assert isinstance(result, list)
