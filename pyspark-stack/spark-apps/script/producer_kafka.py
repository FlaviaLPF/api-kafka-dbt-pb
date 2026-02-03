import os
import requests
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from hdfs import InsecureClient

# =========================
# Configurações
# =========================
OPEN_SKY_URL = "https://opensky-network.org/api/states/all"
TOPIC = "flights_sp"

# HDFS
HDFS_URL = "http://hdfs-namenode:9870"
HDFS_BASE_PATH = "/data/flights/raw"

# Logging
LOG_FILE = "/opt/spark-apps/logs/flights_producer.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

# =========================
# Data de execução (Airflow)
# =========================
EXECUTION_DATE = os.getenv("EXECUTION_DATE")

if not EXECUTION_DATE:
    EXECUTION_DATE = datetime.utcnow().strftime("%Y-%m-%d")

HDFS_PATH = f"{HDFS_BASE_PATH}/dt={EXECUTION_DATE}"
HDFS_FILE = f"{HDFS_PATH}/flights.json"

# =========================
# Aeroportos SP
# =========================
AIRPORTS_SP = {
    "SBGR": {"lat": -23.43, "lon": -46.47},
    "SBSP": {"lat": -23.63, "lon": -46.65},
    "SBKP": {"lat": -23.01, "lon": -47.13},
}

# =========================
# Funções
# =========================
def is_near_airport(lat, lon, airport, max_distance=0.2):
    return abs(lat - airport["lat"]) < max_distance and abs(lon - airport["lon"]) < max_distance


def fetch_opensky_data():
    logging.info(f"Coletando dados OpenSky | dt={EXECUTION_DATE}")

    #response = requests.get(OPEN_SKY_URL, timeout=10)
    
    try:
        
        response = requests.get(OPEN_SKY_URL, timeout=10)        

        if response.status_code == 429:
                logging.warning("Rate limit OpenSky atingido (429). Execução ignorada.")
                return
    
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao acessar OpenSky: {e}")
        return

    data = response.json()
    flights = data.get("states", [])

    # Filtra voos SP
    sp_flights = []
    for f in flights:
        lat, lon = f[6], f[5]
        if lat is None or lon is None:
            continue
        if any(is_near_airport(lat, lon, ap) for ap in AIRPORTS_SP.values()):
            sp_flights.append(f)

    logging.info(f"Voos SP filtrados: {len(sp_flights)}")

    # =========================
    # HDFS
    # =========================
    client = InsecureClient(HDFS_URL, user="airflow")
    client.makedirs(HDFS_PATH)

    with client.write(HDFS_FILE, encoding="utf-8", overwrite=True) as writer:
        json.dump(sp_flights, writer)

    logging.info(f"Arquivo salvo no HDFS: {HDFS_FILE}")

    # =========================
    # Kafka
    # =========================
    producer = KafkaProducer(
        bootstrap_servers=["broker:29092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for flight in sp_flights:
        producer.send(TOPIC, flight)

    producer.flush()
    logging.info(f"{len(sp_flights)} mensagens enviadas para Kafka")


if __name__ == "__main__":
    fetch_opensky_data()
