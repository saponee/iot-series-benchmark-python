from query_benchmark import run_query_influx, run_query_timescale
import os
from dotenv import load_dotenv
from graphs_query import analyze_and_plot_results_query



load_dotenv()

# Numero di ripetizioni per ogni query
REPEAT_PER_QUERY = 3

# Definizione delle query Flux di benchmark

bucket = os.getenv("INFLUX_BUCKET")

QUERIES_FLUX = {
    "mean_temperature": f'''
        from(bucket: "{bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r["_measurement"] == "sensor_data")
          |> filter(fn: (r) => r["_field"] == "temperature")
          |> group(columns: ["device"])
          |> mean()
    ''',
    "count_all_records_for_each_device": f'''
        from(bucket: "{bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r["_measurement"] == "sensor_data")
          |> count()
    ''',
    "days_of_max_temperature":
    f'''
timeRange = -30d
maxTemp = from(bucket: "{bucket}")
  |> range(start: timeRange)
  |> filter(fn: (r) =>
    r._measurement == "sensor_data" and
    r._field == "temperature"
  )
  |> group(columns: ["device"])
  |> max(column: "_value")
  |> rename(columns:{{_value: "max_temp"}})

data = from(bucket: "{bucket}")
  |> range(start: timeRange)
  |> filter(fn: (r) =>
    r._measurement == "sensor_data" and
    r._field == "temperature"
  )

table_joined = join(
  tables: {{data: data, max: maxTemp}},
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
    "mean_temperature":
    '''
    SELECT device,
        AVG(temperature) AS mean_temperature
    FROM sensors
    WHERE time >= NOW() - INTERVAL '30 days'
    GROUP BY device;
    ''',

    "count_all_records_for_each_device": 
    '''
    SELECT device, COUNT(*)
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
  WHERE time >= NOW() - INTERVAL '30 days'
  GROUP BY device
)

SELECT s.device, s.time, s.temperature
FROM sensors s
JOIN max_values mv
  ON s.device = mv.device
WHERE 
  s.time >= NOW() - INTERVAL '30 days' AND
  s.temperature = mv.max_temp;
    '''
}

def main():
    if os.path.exists("query_results.csv"):
        os.remove("query_results.csv")
        print(" File 'query_results.csv' precedente rimosso.")

    for name, flux_query in QUERIES_FLUX.items():
        print(f"\n Avvio test di benchmark per la query --> '{name}' (ripetuto {REPEAT_PER_QUERY} volte)")

        for i in range(REPEAT_PER_QUERY):
            print(f" Esecuzione {i+1} di {REPEAT_PER_QUERY} per la query '{name}'")
        
            run_query_influx(flux_query, name)
    
    for name_ts, ts_query in QUERIES_TS.items() :
        print(f"\n Avvio test di benchmark per la query sql --> '{name_ts}' (ripetuto {REPEAT_PER_QUERY} volte)")
        for i in range(REPEAT_PER_QUERY):
            print(f" Esecuzione {i+1} di {REPEAT_PER_QUERY} per la query '{name}'")

            run_query_timescale(ts_query, name_ts)


    print("\n✅ Tutti i benchmark delle query sono stati completati con successo.")


if __name__ == "__main__":
    main()
    analyze_and_plot_results_query()

