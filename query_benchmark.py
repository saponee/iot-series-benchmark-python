import os
import time
from influxdb_client import InfluxDBClient
from device import save_query_result
from typing import Optional
from dotenv import load_dotenv
import psycopg2 


load_dotenv()


def run_query_influx(flux_query: str, query_name_influx: str) -> Optional[list]:
    """
    Esegue una query InfluxDB, misura la durata e salva il risultato.

    Parametri:
        - flux_query: stringa Flux della query
        - query_name_influx: nome descrittivo della query da salvare nei risultati

    Ritorna:
        - Lista dei risultati della query 
    """
    influx_url = os.getenv("INFLUX_URL")
    influx_token = os.getenv("INFLUX_TOKEN")
    influx_org = os.getenv("INFLUX_ORG")

    if not all([influx_url, influx_token, influx_org]):
        print("Variabili d'ambiente mancanti: verifica INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG")
        return None

    print(f" Connessione a InfluxDB ({influx_url}) in corso...")

    try:
        influx_client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org,
            timeout=3000_000 
        )
        query_api = influx_client.query_api()
    except Exception as e:
        print(f"Connessione a InfluxDB fallita: {e}")
        return None

    print(f" Esecuzione query '{query_name_influx}'...")

    result = None
    duration_query_influx = None

    try:
        start_time = time.perf_counter()
        result = list(query_api.query(flux_query))
        end_time = time.perf_counter()
        duration_query_influx = end_time - start_time

        print(f"✅ Query '{query_name_influx}' completata in {duration_query_influx:.4f}s")
        save_query_result("InfluxDB", query_name_influx, duration_query_influx)

        if not result:
            print("Nessun risultato restituito dalla query.")

    except Exception as e:
        print(f"Errore durante la query '{query_name_influx}': {e}")
    finally:
        influx_client.close()

    return result



def run_query_timescale(timescale_query: str, query_name_ts: str):
    ts_host = os.getenv("DB_HOST")
    ts_port = os.getenv("DB_PORT")
    ts_dbname = os.getenv("DB_NAME")
    ts_password = os.getenv("DB_PASSWORD")
    ts_user = os.getenv("DB_USER")

    try:
        conn = psycopg2.connect(
            user=ts_user,
            password=ts_password,
            host=ts_host,
            port=ts_port,
            dbname=ts_dbname
        )
    except psycopg2.Error as e:
        print(f"Errore TimescaleDB (psycopg2) durante la connessione: {e}")
        return None
    except Exception as e:
        print(f"Errore generico TimescaleDB durante la connessione: {e}")
        return None

    result = None
    duration_query_ts = None

    try:
        with conn.cursor() as cursor:
            start_time = time.perf_counter()
            cursor.execute(timescale_query)

            
            if cursor.description:
                result = cursor.fetchall()

            duration_query_ts = time.perf_counter() - start_time
            conn.commit()
            print(f"✅ Query '{query_name_ts}' completata in {duration_query_ts:.4f} secondi")
            save_query_result('Timescaldb', query_name_ts, duration_query_ts)
    except psycopg2.Error as e:
        print(f"Errore TimescaleDB (psycopg2) durante la query: {e}")
    finally:
        conn.close()

    return result, duration_query_ts





