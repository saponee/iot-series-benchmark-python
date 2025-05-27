import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extras # Aggiunto per execute_values
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.rest import ApiException as InfluxApiException
from requests.exceptions import ConnectionError as RequestsConnectionError

# Carica variabili d'ambiente dal file .env
load_dotenv()

# Configurazione InfluxDB (recuperata dalle variabili d'ambiente)
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# Configurazione TimescaleDB (recuperata dalle variabili d'ambiente)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Modificato per accettare desired_batch_size
def connect_to_influx(desired_batch_size):
    """
    Stabilisce la connessione a InfluxDB e verifica la sua salute.
    Ritorna il client InfluxDB e un oggetto WriteApi configurato per il batching.
    In caso di fallimento, stampa un messaggio di errore e ritorna (None, None).
    """
    client = None
    write_api = None
    try:
        print("Verificando lo stato di InfluxDB...")
        # Configura WriteOptions con la dimensione del batch desiderata
        # flush_interval: il tempo massimo (in ms) che i punti rimangono nel buffer prima di essere scritti
        # batch_size: il numero massimo di punti da tenere nel buffer prima di scriverli
        write_api = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG
        ).write_api(write_options=WriteOptions(batch_size=desired_batch_size, flush_interval=1000)) # 
        
        # Test della connessione leggendo lo stato
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        client.ping()
        print("> Verificando lo stato di InfluxDB... ok")
        print("✅ Connessione a InfluxDB riuscita e API di scrittura configurata per batching.")
        return client, write_api
    except InfluxApiException as e:
        print(f"❌ Errore API InfluxDB durante la connessione: {e}")
    except RequestsConnectionError as e:
        print(f"❌ Errore di connessione a InfluxDB: {e}. Assicurati che il server sia in esecuzione e accessibile all'URL {INFLUX_URL}.")
    except Exception as e:
        print(f"❌ Errore generico durante la connessione a InfluxDB: {e}")
    return None, None

def send_batch_to_influxdb(data_batch, write_api, bucket, org):
    """
    Invia un batch di dati a InfluxDB.
    """
    points = []
    for data in data_batch:
        point = Point("sensor_data") \
            .tag("device", data["device"]) \
            .field("temperature", data["temperature"]) \
            .field("humidity", data["humidity"]) \
            .time(data["timestamp"])
        points.append(point)
    
    try:
        # L'API di scrittura è già configurata per il batching, basta chiamare write
        write_api.write(bucket=bucket, org=org, record=points)
    except InfluxApiException as e:
        print(f"❌ Errore InfluxDB (API) durante l'invio batch dati: {e}")
    except Exception as e:
        print(f"❌ Errore generico InfluxDB durante l'invio batch dati: {e}")


def connect_to_timescale():
    """
    Stabilisce la connessione a TimescaleDB.
    Ritorna l'oggetto connessione o None in caso di fallimento.
    """
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME
        )
        # La creazione della tabella dovrebbe avvenire una volta sola, all'avvio del DB o tramite script di migrazione.
        # Per semplicità in questo script, creiamo la tabella se non esiste.
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensors (
                    time TIMESTAMPTZ NOT NULL,
                    device TEXT NOT NULL,
                    temperature DOUBLE PRECISION,
                    humidity DOUBLE PRECISION
                );
                SELECT create_hypertable('sensors', 'time', if_not_exists => TRUE);
            """)
            conn.commit()
        return conn
    except psycopg2.Error as e:
        print(f"❌ Errore TimescaleDB (psycopg2) durante la connessione: {e}")
        return None
    except Exception as e:
        print(f"❌ Errore generico TimescaleDB durante la connessione: {e}")
        return None

# Modificato per accettare batch_size_from_main
def send_batch_to_timescaledb(data_batch, conn, batch_size_from_main): # <--- MODIFICATO QUI
    """
    Invia un batch di dati a TimescaleDB usando psycopg2.extras.execute_values per inserimenti efficienti.
    """
    if not data_batch:
        return # Non fare nulla se il batch è vuoto

    # Prepara i dati per l'INSERT
    values = [(d["timestamp"], d["device"], d["temperature"], d["humidity"]) for d in data_batch]

    try:
        with conn.cursor() as cursor:
            # Per execute_values, la query INSERT deve avere UN SOLO placeholder %s
            # che verrà poi riempito con le righe di dati da 'values'
            query = """
                INSERT INTO sensors (time, device, temperature, humidity)
                VALUES %s
            """
            # Utilizza il batch_size_from_main passato come page_size
            extras.execute_values(cursor, query, values, page_size=batch_size_from_main) # <--- MODIFICATO QUI
            conn.commit() # Esegui il commit di tutta la transazione per il batch
    except psycopg2.Error as e:
        print(f"❌ Errore TimescaleDB (psycopg2) durante l'invio batch dati: {e}")
        try:
            conn.rollback() # Esegue il rollback in caso di errore
        except psycopg2.Error as rb_error:
            print(f"❌ Errore durante il rollback della transazione TimescaleDB: {rb_error}")
    except Exception as e:
        print(f"❌ Errore generico TimescaleDB durante l'invio batch dati: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback() # Esegue il rollback anche per errori generici
            except psycopg2.Error as rb_error:
                print(f"❌ Errore durante il rollback della transazione TimescaleDB: {rb_error}")