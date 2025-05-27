import time
import os
import pandas as pd
import random
from graphs import analyze_and_plot_results
from datetime import datetime, timedelta, timezone
from device import Device
from sensors import connect_to_influx, connect_to_timescale, send_batch_to_influxdb, send_batch_to_timescaledb

BATCH_SIZE = 100
TEST_VOLUMES = [1, 10, 100, 10000, 50000, 100000]
REPEAT_PER_TEST = 3  # Numero di ripetizioni per ogni test

def save_performance_result(database_name, num_records, duration, throughput):
    results = {
        'database': database_name,
        'num_records': num_records,
        'duration_seconds': duration,
        'throughput_records_per_second': throughput
    }
    df_results = pd.DataFrame([results])
    file_exists = os.path.isfile('performance_results.csv')
    df_results.to_csv('performance_results.csv', mode='a', header=not file_exists, index=False)
    print(f"✔️ Risultati salvati in performance_results.csv per {database_name} ({num_records} record).")

def run_test(num_records_generated):
    print(f"\n--- Inizio test per {num_records_generated} record ---")
    print(f"⏳ Preparando {num_records_generated} dati di sensori...")

    all_data = []
    devices = [Device() for _ in range(10)]
    current_timestamp = datetime.now(timezone.utc)

    for _ in range(num_records_generated):
        device = random.choice(devices)
        all_data.append(device.generate_data(timestamp=current_timestamp))
        current_timestamp += timedelta(milliseconds=1)

    print(f"✔️ {len(all_data)} dati generati in memoria.")

    duration_influx = throughput_influx = None
    duration_ts = throughput_ts = None

    print(f"🌍 Tentativo di inizializzare il client InfluxDB per URL: {os.getenv('INFLUX_URL')}...")
    influx_client, influx_write_api = connect_to_influx(BATCH_SIZE)
    if influx_client and influx_write_api:
        print(f"\n🚀 Avvio inserimento dati in InfluxDB per {num_records_generated} record...")
        start_time_influx = time.perf_counter()

        current_batch_influx = []
        for data_record in all_data:
            current_batch_influx.append(data_record)
            if len(current_batch_influx) >= BATCH_SIZE:
                send_batch_to_influxdb(current_batch_influx, influx_write_api, os.getenv("INFLUX_BUCKET"), os.getenv("INFLUX_ORG"))
                current_batch_influx.clear()
        if current_batch_influx:
            send_batch_to_influxdb(current_batch_influx, influx_write_api, os.getenv("INFLUX_BUCKET"), os.getenv("INFLUX_ORG"))

        end_time_influx = time.perf_counter()
        influx_write_api.close()
        influx_client.close()

        duration_influx = (end_time_influx - start_time_influx)
        throughput_influx = (num_records_generated / duration_influx) if duration_influx > 0 else 0
        print(f"✅ InfluxDB - Completato. Tempo: {duration_influx:.2f} s, Throughput: {throughput_influx:.2f} r/s")
    else:
        print("❌ Connessione InfluxDB fallita, saltando il test.")

    ts_conn = connect_to_timescale()
    if ts_conn:
        print("✅ Connessione a TimescaleDB riuscita.")
        print(f"\n🚀 Avvio inserimento dati in TimescaleDB per {num_records_generated} record...")
        start_time_ts = time.perf_counter()

        current_batch_ts = []
        for data_record in all_data:
            current_batch_ts.append(data_record)
            if len(current_batch_ts) >= BATCH_SIZE:
                send_batch_to_timescaledb(current_batch_ts, ts_conn, BATCH_SIZE)
                current_batch_ts.clear()
        if current_batch_ts:
            send_batch_to_timescaledb(current_batch_ts, ts_conn, BATCH_SIZE)

        end_time_ts = time.perf_counter()
        duration_ts = (end_time_ts - start_time_ts)
        throughput_ts = (num_records_generated / duration_ts) if duration_ts > 0 else 0
        print(f"✅ TimescaleDB - Completato. Tempo: {duration_ts:.2f} s, Throughput: {throughput_ts:.2f} r/s")
        ts_conn.close()
    else:
        print("❌ Connessione TimescaleDB fallita, saltando il test.")

    print(f"--- Fine test per {num_records_generated} record ---")
    return duration_influx, throughput_influx, duration_ts, throughput_ts

def main():
    if os.path.exists('performance_results.csv'):
        os.remove('performance_results.csv')
        print("🗑️ File 'performance_results.csv' precedente rimosso.")

    for volume in TEST_VOLUMES:
        print(f"\n📊 Inizio test per {volume} record (ripetuto {REPEAT_PER_TEST} volte)...")
        influx_durations, influx_throughputs = [], []
        ts_durations, ts_throughputs = [], []

        for run in range(REPEAT_PER_TEST):
            print(f"\n🔁 Esecuzione {run+1} di {REPEAT_PER_TEST} per {volume} record")
            dur_i, thr_i, dur_t, thr_t = run_test(volume)
            if dur_i and thr_i:
                influx_durations.append(dur_i)
                influx_throughputs.append(thr_i)
            if dur_t and thr_t:
                ts_durations.append(dur_t)
                ts_throughputs.append(thr_t)

        if influx_durations:
            avg_dur_i = sum(influx_durations) / len(influx_durations)
            avg_thr_i = sum(influx_throughputs) / len(influx_throughputs)
            save_performance_result('InfluxDB', volume, avg_dur_i, avg_thr_i)

        if ts_durations:
            avg_dur_t = sum(ts_durations) / len(ts_durations)
            avg_thr_t = sum(ts_throughputs) / len(ts_throughputs)
            save_performance_result('TimescaleDB', volume, avg_dur_t, avg_thr_t)

    print("\n✅ Tutte le simulazioni di inserimento completate.")

if __name__ == "__main__":
    main()
    analyze_and_plot_results()
