import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extras # Aggiunto per execute_values
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import WriteType
from influxdb_client.rest import ApiException as InfluxApiException
from requests.exceptions import ConnectionError as RequestsConnectionError

# Carica variabili d'ambiente dal file .env
load_dotenv()

# Configurazione InfluxDB (recupera le variabili di ambiente nel file .env)
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# Configurazione TimescaleDB (recupera le variabili di ambiente nel file .env)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


"""  CONNESSIONI AI DATABASE  """

def connect_to_influx(input_batch_size):
    """
    Stabilisce la connessione a InfluxDB e ne verifica lo stato gestendone le eccezioni.
    Ritorna il client InfluxDB e un oggetto WriteApi (che gestisce le scritture) configurato per il batching.
    In caso di fallimento, stampa un messaggio di errore e ritorna (None, None).
    """


    client = None
    write_api = None


    try:
        print("Verificando lo stato di InfluxDB...")
        
        # flush_interval: il tempo massimo (in ms) che i punti rimangono nel buffer prima di essere scritti
        # batch_size: il numero massimo di punti da tenere nel buffer prima di scriverli
        write_api = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG
        ).write_api(WriteOptions(
    write_type=WriteType.batching,
    batch_size=input_batch_size,
    flush_interval=1000
))
        
        # Test della connessione 
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

        client.ping()

        print(" Verifico lo stato di InfluxDB...")

        print("✅ Connessione a InfluxDB riuscita e API di scrittura configurata per il batching.")
        
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
    Invia un singolo batch di dati a InfluxDB e ne cattura le eccezioni.
    """

    data_points = []

    for data in data_batch:
        data_point = Point("sensor_data") \
            .tag("device", data["device"]) \
            .field("temperature", data["temperature"]) \
            .field("humidity", data["humidity"]) \
            .time(data["timestamp"])
        data_points.append(data_point)
    
    try:
        
        write_api.write(bucket=bucket, org=org, record=data_points)

    except InfluxApiException as e:
        print(f"❌ Errore InfluxDB (API) durante l'invio batch dati: {e}")
    except Exception as e:
        print(f"❌ Errore generico InfluxDB durante l'invio batch dati: {e}")


def connect_to_timescale():
    """
    Stabilisce la connessione a TimescaleDB.
    Ritorna l'oggetto connessione o None in caso di fallimento catturandone le eccezioni.
    """
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME
        )
        return conn
    

    except psycopg2.Error as e:
        print(f"❌ Errore TimescaleDB (psycopg2) durante la connessione: {e}")
        return None
    except Exception as e:
        print(f"❌ Errore generico TimescaleDB durante la connessione: {e}")
        return None




def send_batch_to_timescaledb(data_batch, conn, input_batch_size): 
    """
    Invia un batch di dati a TimescaleDB.
    """


    if not data_batch:
        return 

    # Prepara i dati per l'INSERT
    values = [  (  d["timestamp"], d["device"], d["temperature"], d["humidity"]  ) for d in data_batch]

    try:
        with conn.cursor() as cursor:
            

            query = """
                INSERT INTO sensors (time, device, temperature, humidity)
                VALUES %s
            """
            
            extras.execute_values(cursor, query, values, page_size=input_batch_size)
             # execute_values: permette di inserire più righe contemporaneamente (in batch), migliorando l'efficienza
             # Per questo c'è un solo place holder %s
             # execute_values accetta una lista di tuple in input ed ogni tupla contiene tutti i valori da inserire (values). 



            conn.commit() 


        
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
                conn.rollback() # Esegue il rollback 
            except psycopg2.Error as rb_error:
                print(f"❌ Errore durante il rollback della transazione TimescaleDB: {rb_error}")