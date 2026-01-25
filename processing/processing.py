import pandas as pd
import os

SOURCE_DIR = os.getenv("SOURCE_DIR", "/data/raw") 
TARGET_FILE = os.getenv("TARGET_FILE", "/data/processed/stats.parquet")

def aggregate_data():
    print("Lade Rohdaten...")
    if not os.path.exists(SOURCE_DIR):
        print("Keine Rohdaten gefunden. Ingestion zuerst laufen lassen!")
        return

    try:
        # Pandas/PyArrow kann einen Ordner lesen und hängt automatisch alle parquets zusammen
        df = pd.read_parquet(SOURCE_DIR, engine='pyarrow')
    except Exception as e:
        print(f"Fehler beim Lesen der Parquet-Dateien: {e}")
        return

    # Einfache Transformation: Filtern und Aggregieren
    print(f"Daten geladen: {len(df)} Zeilen.")
    print("Verarbeite Daten...")
    
    # Filter: Nur gewertete Spiele (kein Elo 0)
    df_clean = df[(df['WhiteElo'] > 0) & (df['BlackElo'] > 0)]

    # Feature Engineering: Hat Weiß gewonnen?
    # Result ist '1-0' (Weiß gewinnt), '0-1' (Schwarz gewinnt), '1/2-1/2' (Remis)
    df_clean['WhiteWin'] = df_clean['Result'] == '1-0'

    # Aggregation nach Eröffnung (ECO)
    stats = df_clean.groupby('ECO').agg(
        TotalGames=('Result', 'count'),
        WhiteWinRate=('WhiteWin', 'mean'),
        AvgWhiteElo=('WhiteElo', 'mean')
    ).reset_index()

    # Nur Eröffnungen mit mindestens 10 Partien behalten
    stats = stats[stats['TotalGames'] >= 10].sort_values(by='TotalGames', ascending=False)

    print(f"Speichere aggregierte Daten nach {TARGET_FILE}...")
    # Ordner erstellen
    os.makedirs(os.path.dirname(TARGET_FILE), exist_ok=True)
    stats.to_parquet(TARGET_FILE)
    print("Processing abgeschlossen.")

if __name__ == "__main__":
    aggregate_data()