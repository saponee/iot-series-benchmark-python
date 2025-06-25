import time
import os
import pandas as pd
import random
from graphs_datapoints import analyze_and_plot_results
from datetime import datetime, timedelta, timezone
from device import Device, save_performance_result
from sensors import connect_to_influx, connect_to_timescale, send_batch_to_influxdb, send_batch_to_timescaledb


# Paramentri per il testing 

BATCH_SIZE = 1000
DATA_VOLUMES = [1000]
REPEAT_PER_TEST = 1  # Numero di ripetizioni per ogni test


def run_test(num_records_generated):
    """
    Esegue il test per il numero dei volume di dati richiesto,
    misurandone le prestazioni per il throughput e il tempo di esecuzione.
    """
    print(f"\n------ Inizio test per {num_records_generated} record ------")
    print(f" Preparando:  {num_records_generated} dati di sensori...")

    all_data = []

    devices = []
    for _ in range(10):
        device = Device()
        devices.append(device)

    current_timestamp = datetime.now(timezone.utc)

    for _ in range(num_records_generated):

        device = random.choice(devices)

        all_data.append(device.generate_data(timestamp=current_timestamp))

        current_timestamp += timedelta(milliseconds=1)  

    print(f" {len(all_data)} dati generati in memoria.")

    # INIZIO INFLUX

    duration_influx = throughput_influx = None
    duration_ts = throughput_ts = None 

    print(f" Tentativo di connessione al client InfluxDB per --> {os.getenv('INFLUX_URL')}...")

    influx_client, influx_write_api = connect_to_influx(BATCH_SIZE) 

    if influx_client and influx_write_api:
        try:
            print(f"\n Avvio inserimento dati in InfluxDB per {num_records_generated} record...")

            start_time_influx = time.perf_counter() # INIZIO COUNTER TEMPORALE

            current_batch_influx = []

            # CREAZIONE DEI BATCH PER INFLUX
            for data_record in all_data:
                current_batch_influx.append(data_record)
                if len(current_batch_influx) >= BATCH_SIZE:
                    send_batch_to_influxdb(current_batch_influx, influx_write_api, os.getenv("INFLUX_BUCKET"), os.getenv("INFLUX_ORG"))
                    current_batch_influx.clear()

            
            if current_batch_influx:
                send_batch_to_influxdb(current_batch_influx, influx_write_api, os.getenv("INFLUX_BUCKET"), os.getenv("INFLUX_ORG"))

            end_time_influx = time.perf_counter() 

            # Calcola le metriche
            duration_influx = (end_time_influx - start_time_influx)
            throughput_influx = (num_records_generated / duration_influx) if duration_influx > 0 else 0
            print(f" InfluxDB - Completato. Tempo: {duration_influx:.2f} s, Throughput: {throughput_influx:.2f} r/s")

        except Exception as e:
            
            print(f"Errore critico durante l'inserimento in InfluxDB: {e}")
        finally:
            
            if influx_write_api: 
                print("Chiusura di influx_write_api e svuotamento del buffer.")
                influx_write_api.close()
            if influx_client: 
                print("Chiusura della connessione influx_client.")
                influx_client.close()

    else:
        print("Connessione InfluxDB fallita, test saltato.")
#FINE INFLUX


     # --- INIZIO TS ---
    print(f"\nTentativo di connessione a TimescaleDB...")
    ts_conn = connect_to_timescale()

    if ts_conn:
        try:
            print("Connessione a TimescaleDB riuscita.")
            print(f"\n Avvio inserimento dati in TimescaleDB per {num_records_generated} record...")

            # INIZIO DEL COUNTER
            start_time_ts = time.perf_counter()

            current_batch_ts = [] # CARICAMENTO DEL BATCH

            for data_record in all_data:
                current_batch_ts.append(data_record)
                if len(current_batch_ts) >= BATCH_SIZE:
                    send_batch_to_timescaledb(current_batch_ts, ts_conn, BATCH_SIZE)
                    current_batch_ts.clear()

            # Invia l'ultimo batch residuo
            if current_batch_ts:
                send_batch_to_timescaledb(current_batch_ts, ts_conn, BATCH_SIZE)

            
            end_time_ts = time.perf_counter()

            
            duration_ts = (end_time_ts - start_time_ts)
            throughput_ts = (num_records_generated / duration_ts) if duration_ts > 0 else 0
            print(f" TimescaleDB - Completato. Tempo: {duration_ts:.2f} s, Throughput: {throughput_ts:.2f} r/s")

        except Exception as e:
            # Cattura qualsiasi errore imprevisto durante l'inserimento in TimescaleDB
            print(f" Errore critico durante l'inserimento in TimescaleDB: {e}")
        finally:
            # QUESTO BLOCCO GARANTISCE LA CHIUSURA
            if ts_conn:
                print("Chiusura della connessione TimescaleDB.")
                ts_conn.close()
    else:
        print("Connessione TimescaleDB fallita, test salatato.")

    print(f"\n------ Fine test per {num_records_generated} record ------")

    return duration_influx, throughput_influx, duration_ts, throughput_ts



def main():
    """
    Provvede a lanciare il programma ripetendo i test e rimuovendo il vecchio .csv 
    facendone poi una media matematica dei risultati.
    
    """
    
    if os.path.exists('performance_results.csv'):
        os.remove('performance_results.csv')
        print(" File 'performance_results.csv' precedente rimosso.")

    for volume in DATA_VOLUMES:
        print(f"\n Inizio test per {volume} record (ripetuto {REPEAT_PER_TEST} volte)...")

        for run in range(REPEAT_PER_TEST):
            print(f"\n Esecuzione {run+1} di {REPEAT_PER_TEST} per {volume} record")
            dur_i, thr_i, dur_t, thr_t = run_test(volume)

            if dur_i and thr_i:
                save_performance_result('InfluxDB', volume, dur_i, thr_i)

            if dur_t and thr_t:
                save_performance_result('TimescaleDB', volume, dur_t, thr_t)

    print("\n Tutte le simulazioni di inserimento completate.")


if __name__ == "__main__":
    main()
    analyze_and_plot_results()
