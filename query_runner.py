from query_benchmark import run_query_influx, run_query_timescale
import os
from dotenv import load_dotenv
from graphs_query import analyze_and_plot_results_query



load_dotenv()

# Numero di ripetizioni per ogni query
REPEAT_PER_QUERY = 1

# Definizione delle query Flux di benchmark

bucket = os.getenv("INFLUX_BUCKET")

QUERIES_FLUX = {
    "aggregation_query":f'''
      from(bucket: "{bucket}")
    |> range(start: -14d)
    |> filter(fn: (r) => r["_measurement"] == "sensor_data")
    |> aggregateWindow(every: 1h, fn: count, createEmpty: false)
    |> group(columns: ["device"])
    |> rename(columns: {{_value: "records_per_hour", _time: "device_per_hour"}})
    |> sort(columns: ["device"])

    ''',
    "mean_humidity": f'''
        from(bucket: "{bucket}")
          |> range(start: -14d)
          |> filter(fn: (r) => r["_measurement"] == "sensor_data")
          |> filter(fn: (r) => r["_field"] == "humidity")
          |> group(columns: ["device"])
          |> mean()
    ''',
    "count_records_for_each_device": f'''
        from(bucket: "{bucket}")
          |> range(start: -14d)
          |> filter(fn: (r) => r["_measurement"] == "sensor_data")
          |> group(columns: ["device"])
          |> count()
          |> rename(columns : {{_value:"devices_counted"}})
    ''',
    "join_counter":
    f'''
  temp = from(bucket: "{bucket}")
  |> range(start: -14d)
  |> filter(fn: (r) => r._measurement == "sensor_data" and r._field == "temperature")
  |> aggregateWindow(every: 1m, fn: max, createEmpty: false)
  |> rename(columns: {{_value: "max_temperature"}})

hum = from(bucket: "{bucket}")
  |> range(start: -14d)
  |> filter(fn: (r) => r._measurement == "sensor_data" and r._field == "humidity")
  |> aggregateWindow(every: 1m, fn: max, createEmpty: false)
  |> rename(columns: {{_value: "max_humidity"}})

joined = join(
  tables: {{t: temp, h: hum}},
  on: ["_time", "device"],
  method: "inner"
)
  |> keep(columns: ["_time", "device", "max_temperature", "max_humidity"])

joined
  |> group(columns: ["device"])
  |> count(column: "max_temperature")
  |> rename(columns: {{max_temperature: "counter"}}
  )

'''
}

QUERIES_TS = {
    "aggregation_query":'''
    SELECT time_bucket('1 hour', time) AS device_per_hour,
       device,
       COUNT(*) AS records_per_hour
        FROM sensors
        WHERE time >= NOW() - INTERVAL '14 days'
        GROUP BY device_per_hour, device
        ORDER BY  device;''',

    "mean_humidity":
    '''
    SELECT device,
    AVG(humidity) AS mean_humidity
    FROM sensors
    WHERE time >= NOW() - INTERVAL '14 days'
    GROUP BY device;
    ''',

    "count_records_for_each_device": 
    '''
    SELECT device, COUNT(*) as devices_counted
    FROM sensors
    GROUP BY device;
    '''
    ,
    "join_counter":

    '''
    WITH max_temp AS (
    SELECT 
        time_bucket('1 minutes', time) AS bucket,
        device,
        MAX(temperature) AS max_temperature
    FROM sensors
    WHERE time > now() - interval '14 days'
    GROUP BY bucket, device
),

max_hum AS (
    SELECT 
        time_bucket('1 minutes', time) AS bucket,
        device,
        MAX(humidity) AS max_humidity
    FROM sensors
    WHERE time > now() - interval '14 days'
    GROUP BY bucket, device
),

joined AS (
    SELECT 
        t.bucket, 
        t.device
    FROM max_temp t
    JOIN max_hum h
        ON t.bucket = h.bucket
        AND t.device = h.device
)

SELECT
    device,
    COUNT(*) AS counter
FROM joined
GROUP BY device
ORDER BY device;
    '''
    
    
}

def main():
    if os.path.exists("query_results.csv"):
        os.remove("query_results.csv")
        print(" File 'query_results.csv' precedente rimosso.")

    for name, flux_query in QUERIES_FLUX.items():
        print(f"\n Avvio test di benchmark per la query --> '{name}'")

        for i in range(REPEAT_PER_QUERY):        
            run_query_influx(flux_query, name)
    
    for name_ts, ts_query in QUERIES_TS.items() :
        print(f"\n Avvio test di benchmark per la query sql --> '{name_ts}'")
        for i in range(REPEAT_PER_QUERY):
            run_query_timescale(ts_query, name_ts)


    print("\nTutti i test riguardanti le query sono stati completati.")


if __name__ == "__main__":
    main()
    analyze_and_plot_results_query()

