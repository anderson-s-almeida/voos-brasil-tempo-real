import os, json, time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_URL = "https://opensky-network.org/api/states/all"
BBOX = {"lamin": -34.0, "lomin": -74.0, "lamax": 5.5, "lomax": -34.0}
INTERVALO = 120                                  # segundos entre coletas
PASTA_RAW = "dados/raw"
VOLUME = "/Volumes/workspace/voos/raw"

_token = {"valor": None, "expira": 0}
pendentes = []                                   # arquivos que falharam no envio
dbx = WorkspaceClient()                          # usa DATABRICKS_HOST e DATABRICKS_TOKEN do .env


def obter_token():
    """Retorna um token valido da OpenSky, renovando 1 minuto antes de expirar."""
    if _token["valor"] and time.time() < _token["expira"] - 60:
        return _token["valor"]
    r = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": os.getenv("OPENSKY_CLIENT_ID"),
        "client_secret": os.getenv("OPENSKY_CLIENT_SECRET"),
    }, timeout=30)
    r.raise_for_status()
    corpo = r.json()
    _token["valor"] = corpo["access_token"]
    _token["expira"] = time.time() + corpo.get("expires_in", 1800)
    print("  token renovado")
    return _token["valor"]


def enviar_databricks(arquivo_local):
    """Envia o JSON para o volume, mantendo a pasta da data."""
    data, nome = arquivo_local.replace("\\", "/").split("/")[-2:]
    destino = f"{VOLUME}/{data}/{nome}"
    with open(arquivo_local, "rb") as f:
        dbx.files.upload(destino, f, overwrite=True)
    return destino


def reenviar_pendentes():
    """Tenta novamente os arquivos que falharam antes."""
    for arquivo in pendentes[:]:
        try:
            enviar_databricks(arquivo)
            pendentes.remove(arquivo)
            print(f"  reenviado: {arquivo}")
        except Exception:
            break


def coletar():
    """Faz uma coleta, salva local e envia ao Databricks. Retorna segundos extras de espera."""
    headers = {"Authorization": f"Bearer {obter_token()}"}
    r = requests.get(API_URL, params=BBOX, headers=headers, timeout=60)

    if r.status_code == 401:                     # token invalido: forca renovacao
        _token["valor"] = None
        headers = {"Authorization": f"Bearer {obter_token()}"}
        r = requests.get(API_URL, params=BBOX, headers=headers, timeout=60)

    if r.status_code == 429:                     # cota esgotada
        espera = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 3600))
        print(f"  cota esgotada, aguardando {espera} s")
        return espera

    r.raise_for_status()
    dados = r.json()
    agora = datetime.now(timezone.utc)
    dados["_coletado_em"] = agora.isoformat()

    pasta = os.path.join(PASTA_RAW, agora.strftime("%Y-%m-%d"))
    os.makedirs(pasta, exist_ok=True)
    arquivo = os.path.join(pasta, f"posicao_{agora.strftime('%H%M%S')}.json")
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False)

    try:
        enviar_databricks(arquivo)
        status = "enviado"
    except Exception as e:
        pendentes.append(arquivo)
        status = f"FALHA no envio ({e.__class__.__name__}), pendentes: {len(pendentes)}"

    qtd = len(dados.get("states") or [])
    restantes = r.headers.get("X-Rate-Limit-Remaining")
    print(f"{agora:%H:%M:%S} UTC | avioes: {qtd} | creditos: {restantes} | {status}")
    return 0


if __name__ == "__main__":
    print("Coletor iniciado (Ctrl+C para parar)")
    try:
        while True:
            try:
                reenviar_pendentes()
                extra = coletar()
            except Exception as e:
                print("  erro na coleta:", e)
                extra = 0
            time.sleep(INTERVALO + extra)
    except KeyboardInterrupt:
        print("Coletor finalizado.")