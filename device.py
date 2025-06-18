from datetime import datetime, timezone
import random
import os
import pandas as pd

class Device:
    def __init__(self):
        self.device_id = f"sensor_{random.randint(1, 10)}"

    def generate_data(self, timestamp=None):
        if not timestamp:
            timestamp = datetime.now(timezone.utc)
        return {
            "device": self.device_id,
            "temperature": round(random.uniform(20, 30), 2),
            "humidity": round(random.uniform(30, 60), 2),
            "timestamp": timestamp
        }

def save_query_result(database_name, query_name, duration):
    """
    Salva i risultati del test di query in query_results.csv.
    """
    results_query = {
        'database': database_name,
        'query': query_name,
        'duration_seconds': duration
    }
    file_exists = os.path.isfile('query_results.csv')
    df = pd.DataFrame([results_query])
    df.to_csv('query_results.csv', mode='a', header=not file_exists, index=False)
    print(f"Risultati query '{query_name}' salvati per {database_name}.")

def save_performance_result(database_name, num_records, duration, throughput):
    """
    Genera un file csv con all'interno
    i risultati del test appena concluso, in particolare:
    - nome del database, 
    - numero dei record totali che sono stati inseriti in quel test
    - la durata in secondi dell'invio e in throughput.
    
    """

    results = {
        'database': database_name,
        'num_records': num_records,
        'duration_seconds': duration,
        'throughput_records_per_second': throughput
    }
    df_results = pd.DataFrame([results])
    file_exists = os.path.isfile('performance_results.csv')
    df_results.to_csv('performance_results.csv', mode='a', header=not file_exists, index=False)
    print(f"Risultati salvati per {database_name} ({num_records} record).")
