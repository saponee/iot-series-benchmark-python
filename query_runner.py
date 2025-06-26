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
    "days_of_max_temperature":
    f'''
    timeRange = -14d
    maxTemp = from(bucket: "{bucket}")
  |> range(start: timeRange)
  |> filter(fn: (r) =>
    r._measurement == "sensor_data" and
    r._field == "temperature"
  )
  |> group(columns: ["device"])
  |> max(column: "_value")
  |> rename(columns:{{_value: "max_temp"}})

temperature_data = from(bucket: "{bucket}")
  |> range(start: timeRange)
  |> filter(fn: (r) =>
    r._measurement == "sensor_data" and
    r._field == "temperature"
  )

table_joined = join(
  tables: {{temperature_data: temperature_data, max: maxTemp}},
  on: ["device"]
)
result = table_joined
  |> filter(fn: (r) => r._value == r.max_temp)
  |> keep(columns: ["device", "_time", "_value"])
  |> rename(columns: {{_value: "temperature"}})

result
  |> yield(name: "max_temperature_date")

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
        ORDER BY device_per_hour, device;''',

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
    "days_of_max_temperature":
    '''
    WITH max_values AS (
  SELECT 
    device,
    MAX(temperature) AS max_temp
  FROM sensors
  WHERE time >= NOW() - INTERVAL '14 days'
  GROUP BY device
)

SELECT s.device, s.time, s.temperature
FROM sensors s
JOIN max_values mv
  ON s.device = mv.device
WHERE 
  s.time >= NOW() - INTERVAL '14 days' AND
  s.temperature = mv.max_temp;
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

