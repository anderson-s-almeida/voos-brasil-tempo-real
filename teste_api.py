import os, json, sys
import requests
from dotenv import load_dotenv

load_dotenv()
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_URL = "https://opensky-network.org/api/states/all"

# Retangulo do Brasil
BBOX = {"lamin": -34.0, "lomin": -74.0, "lamax": 5.5, "lomax": -34.0}

# 1. Autenticacao OAuth2 (client credentials)
r = requests.post(TOKEN_URL, data={
    "grant_type": "client_credentials",
    "client_id": os.getenv("OPENSKY_CLIENT_ID"),
    "client_secret": os.getenv("OPENSKY_CLIENT_SECRET"),
})
if r.status_code != 200:
    sys.exit(f"Falha na autenticacao: {r.status_code} {r.text}")
token = r.json()["access_token"]
print("Token OK (expira em", r.json().get("expires_in"), "segundos)")

# 2. Posicoes dos avioes sobre o Brasil
r = requests.get(API_URL, params=BBOX, headers={"Authorization": f"Bearer {token}"})
print("Status:", r.status_code, "| Creditos restantes:", r.headers.get("X-Rate-Limit-Remaining"))
r.raise_for_status()
dados = r.json()

estados = dados.get("states") or []
print("Horario (epoch):", dados["time"], "| Avioes:", len(estados))
for s in estados[:5]:
    print(f"  {(s[1] or '-').strip():8} {s[2]:15} lat={s[6]} lon={s[5]} alt={s[7]} m vel={s[9]} m/s")

with open("amostra_posicao.json", "w", encoding="utf-8") as f:
    json.dump(dados, f, ensure_ascii=False, indent=2)
print("Amostra salva em amostra_posicao.json")