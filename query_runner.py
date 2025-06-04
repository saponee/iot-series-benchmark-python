from query_benchmark import run_query_influx
import os
from dotenv import load_dotenv
from graphs_query import analyze_and_plot_results_query




load_dotenv()

# Numero di ripetizioni per ogni query
REPEAT_PER_QUERY = 3

# Definizione delle query Flux di benchmark

bucket = os.getenv("INFLUX_BUCKET")

QUERIES = {
    "mean_temperature": f'''
        from(bucket: "{bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "sensor_data")
          |> filter(fn: (r) => r["_field"] == "temperature")
          |> group(columns: ["device"])
          |> mean()
    ''',
    "count_records": f'''
        from(bucket: "{bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "sensor_data")
          |> filter(fn: (r) => r["_field"] == "temperature")
          |> count()
    '''

}



def main():
    if os.path.exists("query_results.csv"):
        os.remove("query_results.csv")
        print("🗑️ File 'query_results.csv' precedente rimosso.")

    for name, flux_query in QUERIES.items():
        print(f"\n Avvio test di benchmark per la query --> '{name}' (ripetuto {REPEAT_PER_QUERY} volte)")

        for i in range(REPEAT_PER_QUERY):
            print(f" Esecuzione {i+1} di {REPEAT_PER_QUERY} per la query '{name}'")
            run_query_influx(flux_query, name)

    print("\n✅ Tutti i benchmark delle query sono stati completati.")


if __name__ == "__main__":
    main()
    analyze_and_plot_results_query()

