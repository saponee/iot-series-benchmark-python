import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os


def analyze_and_plot_results_query(file_path='query_results.csv'):
    

    plot_query_time_file = 'average_time_for_query.png'

    removed_plot_duration_file = False 

    if os.path.exists(plot_query_time_file):
        try:
            os.remove(plot_query_time_file)
            removed_plot_duration_file = True
        except Exception as e:
            print(f"⚠️ Attenzione: Impossibile rimuovere '{plot_query_time_file}': {e}")
            removed_plot_duration_file = False
    
    if removed_plot_duration_file == True:
        print(f"🗑️ File '{plot_query_time_file}' rimosso correttamente" )
    elif removed_plot_duration_file == False:
        print(f"🗑️il File '{plot_query_time_file}' non è stato rimosso correttamente" )
    
    if not os.path.exists(file_path):
        print(f"❌ Errore: Il file '{file_path}' non è stato trovato. Assicurati di aver eseguito prima i test.")
        return
    
    
    print(f" Analizzando i risultati dal file: {file_path}...")


    try:
        df = pd.read_csv(file_path)
        print(f"Dati presenti in {plot_query_time_file} letti con successo")
        print(df.head())

    except Exception as e:
        print(f"Errore nella lettura del file CSV: {e}")
        return
    
    df_grouped = df.groupby(['database', 'query']).agg(
        average_duration = ('duration_seconds', 'mean')
    ).reset_index()

    print("\nMedie durata query calcolate:")
    print(df_grouped)

    #  Genera i grafici

    
    sns.set_theme(style="whitegrid")
    plt.style.use('ggplot') 

    # Grafico 1: Durata media (Tempo di Esecuzione)
    plt.figure(figsize=(12, 7)) 
    sns.barplot(x='query', y='average_duration', hue='database', data=df_grouped, palette='viridis')
    plt.title('Durata Media di Esecuzione query per database', fontsize=16)
    plt.xlabel('nome query', fontsize=12)
    plt.ylabel('Durata Media (secondi)', fontsize=12)
    plt.legend(title='Database')
    plt.xticks(rotation=45, ha='right')  
    plt.tight_layout() 
    plt.savefig(plot_query_time_file) 
    print(f"📈 Grafico '{plot_query_time_file}' generato.")


    print("\n✅ Analisi e generazione grafici completate.")

if __name__ == "__main__":
    analyze_and_plot_results_query()

