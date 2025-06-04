import os
import time
from influxdb_client import InfluxDBClient
from device import save_query_result
from typing import Optional
from dotenv import load_dotenv
load_dotenv()


def run_query_influx(flux_query: str, query_name: str) -> Optional[list]:
    """
    Esegue una query InfluxDB, misura la durata e salva il risultato.

    Parametri:
        - flux_query: stringa Flux della query
        - query_name: nome descrittivo della query da salvare nei risultati

    Ritorna:
        - Lista dei risultati della query (può essere vuota o None in caso di errore)
    """
    influx_url = os.getenv("INFLUX_URL")
    influx_token = os.getenv("INFLUX_TOKEN")
    influx_org = os.getenv("INFLUX_ORG")

    if not all([influx_url, influx_token, influx_org]):
        print("❌ Variabili d'ambiente mancanti: verifica INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG")
        return None

    print(f" Connessione a InfluxDB ({influx_url}) in corso...")

    try:
        influx_client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org,
            timeout=60_000 
        )
        query_api = influx_client.query_api()
    except Exception as e:
        print(f"❌ Connessione a InfluxDB fallita: {e}")
        return None

    print(f" Esecuzione query '{query_name}'...")

    result = None
    duration_query_influx = None

    try:
        start_time = time.perf_counter()
        result = list(query_api.query(flux_query))
        end_time = time.perf_counter()
        duration_query_influx = end_time - start_time

        print(f"✅ Query '{query_name}' completata in {duration_query_influx:.4f}s")
        save_query_result("InfluxDB", query_name, duration_query_influx)

        if result:
            print(f"🔍 Risultati trovati: {len(result)}")
        else:
            print("⚠️ Nessun risultato restituito dalla query.")

    except Exception as e:
        print(f"❌ Errore durante la query '{query_name}': {e}")
    finally:
        influx_client.close()

    return result
