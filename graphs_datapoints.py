import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def analyze_and_plot_results(file_path='performance_results.csv'):
    """
    Gestisce la lettura del file .csv eseguendone la media per ciascun
    test eseguito e dopodichè genera i grafici per entrambi.

    """

    plot_duration_file = 'average_duration_plot.png'
    plot_throughput_file = 'average_throughput_plot.png'

    
    removed_plot_duration_file = False
    removed_plot_throughput_file = False

    
    if os.path.exists(plot_duration_file):
        try:
            os.remove(plot_duration_file)
            removed_plot_duration_file = True
        except OSError as e: 
            print(f" Attenzione: Impossibile rimuovere '{plot_duration_file}': {e}")
            removed_plot_duration_file = False 

   
    if os.path.exists(plot_throughput_file):
        try:
            os.remove(plot_throughput_file)
            removed_plot_throughput_file = True
        except OSError as e:
            print(f"Attenzione: Impossibile rimuovere '{plot_throughput_file}': {e}")
            removed_plot_throughput_file = False 

   
    if removed_plot_duration_file and removed_plot_throughput_file:
        print(f" Files '{plot_duration_file}' e '{plot_throughput_file}' precedenti rimossi.")
    elif removed_plot_duration_file or removed_plot_throughput_file:
        print("Solo uno dei file dei grafici precedenti è stato rimosso. Controlla i messaggi di attenzione per dettagli.")


    if not os.path.exists(file_path):
        print(f" Errore: Il file '{file_path}' non è stato trovato. Assicurati di aver eseguito prima i test.")
        return

    print(f" Analizzando i risultati dal file: {file_path}...")

    
    try:
        df = pd.read_csv(file_path)
        print("Dati letti con successo:")
        print(df.head())
    except Exception as e:
        print(f"Errore durante la lettura del file CSV: {e}")
        return

    # Raggruppa i dati per database e numero di record,
    # poi calcola la media della durata e del throughput per ogni gruppo:

    df_grouped = df.groupby(['database', 'num_records']).agg(
        average_duration=('duration_seconds', 'mean'), 
        average_throughput=('throughput_records_per_second', 'mean') 
    ).reset_index() 

    print("\nMedie calcolate:")
    print(df_grouped)

    #  Genera i grafici

    
    sns.set_theme(style="whitegrid")
    plt.style.use('ggplot') 

    # Grafico 1: Durata media (Tempo di Esecuzione)
    plt.figure(figsize=(12, 7)) 
    sns.barplot(x='num_records', y='average_duration', hue='database', data=df_grouped, palette='viridis')
    plt.title('Durata Media di Inserimento Dati per Database e Volume di Record', fontsize=16)
    plt.xlabel('Numero di Record', fontsize=12)
    plt.ylabel('Durata Media (secondi)', fontsize=12)
    plt.legend(title='Database')
    plt.xticks(rotation=45, ha='right')  
    plt.tight_layout() 
    plt.savefig(plot_duration_file) 
    print(f" Grafico '{plot_duration_file}' generato.")
    

    # Grafico 2: Throughput medio (Record al Secondo)
    plt.figure(figsize=(12, 7))
    sns.barplot(x='num_records', y='average_throughput', hue='database', data=df_grouped, palette='magma') 
    plt.title('Throughput Medio di Inserimento Dati per Database e Volume di Record', fontsize=16)
    plt.xlabel('Numero di Record', fontsize=12)
    plt.ylabel('Throughput Medio (record/secondo)', fontsize=12)
    plt.legend(title='Database')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(plot_throughput_file)
    print(f" Grafico '{plot_throughput_file}' generato.")
    # plt.show()

    print("\n Analisi e generazione grafici completate.")

if __name__ == "__main__":
    analyze_and_plot_results()