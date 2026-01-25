import chess.pgn
import zstandard as zstd
import pandas as pd
import io
import os
import glob

# Konfiguration aus Environment Variables (oder Standardwerte)
SOURCE_DIR = os.getenv("SOURCE_DIR", "/data")
FILE_PATTERN = os.getenv("FILE_PATTERN", ".pgn.zst")
TARGET_FILE = os.getenv("TARGET_FILE", "/data/raw_games.parquet")
# Test-Limit laden (Standard: 10.000, bei 0 oder -1 -> kein Limit)
MAX_GAMES = int(os.getenv("MAX_GAMES", 10000))

def get_files():
    # Sucht alle Dateien im Ordner, die auf .pgn.zst enden
    search_path = os.path.join(SOURCE_DIR, f"*{FILE_PATTERN}")
    files = glob.glob(search_path)
    files.sort() # Sortieren für reproduzierbare Reihenfolge
    return files

def process_pgn():
    files = get_files()
    if not files:
        print(f"WARNUNG: Keine Dateien mit Endung '{FILE_PATTERN}' in {SOURCE_DIR} gefunden.")
        return

    print(f"Gefundene Dateien: {files}")
    print(f"Globales Limit gesetzt auf: {MAX_GAMES} Partien")
    
    data = []
    total_games_processed = 0

    # Äußere Schleife: Über alle Dateien iterieren
    for file_path in files:
        # Abbruchbedingung prüfen (wenn Limit erreicht)
        if MAX_GAMES > 0 and total_games_processed >= MAX_GAMES:
            break

        print(f"--> Starte Verarbeitung von: {os.path.basename(file_path)}")
        
        try:
			# Stream öffnen (zstd dekomprimieren)
            with open(file_path, 'rb') as fh:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(fh) as reader:
                    text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                    
                    while True:
                        # Auch hier Abbruch prüfen
                        if MAX_GAMES > 0 and total_games_processed >= MAX_GAMES:
                            break

                        try:
                            game = chess.pgn.read_game(text_stream)
                        except Exception as e:
                            print(f"Fehler beim Parsen einer Partie: {e}")
                            continue

                        if game is None:
                            break # Ende dieser Datei

                        headers = game.headers
                        
						# Anonymisierung
                        row = {
                            'Event': headers.get("Event", "Unknown"),
                            'Result': headers.get("Result", "*"),
                            'WhiteElo': headers.get("WhiteElo", "0"),
                            'BlackElo': headers.get("BlackElo", "0"),
                            'ECO': headers.get("ECO", "Unknown"), # Eröffnungscode
                            'Termination': headers.get("Termination", "Normal")
                        }
                        data.append(row)
                        total_games_processed += 1
                        
                        if total_games_processed % 2000 == 0:
                            print(f"Gesamtfortschritt: {total_games_processed} Partien...")

        except Exception as e:
            print(f"Fehler beim Lesen der Datei {file_path}: {e}")

    print(f"Verarbeitung beendet. Gesamtanzahl Partien: {total_games_processed}")

    if not data:
        print("Keine Daten extrahiert.")
        return
	
	# Als Parquet speichern
    print("Erstelle DataFrame...")
    df = pd.DataFrame(data)
    
    # Data Cleaning - Datentypen anpassen (Elo zu Int)
    df['WhiteElo'] = pd.to_numeric(df['WhiteElo'], errors='coerce').fillna(0).astype(int)
    df['BlackElo'] = pd.to_numeric(df['BlackElo'], errors='coerce').fillna(0).astype(int)

    print(f"Speichere {len(df)} Zeilen nach {TARGET_FILE}...")
    # Ordner erstellen, falls er nicht existiert
    os.makedirs(os.path.dirname(TARGET_FILE), exist_ok=True)
    
    df.to_parquet(TARGET_FILE, engine='pyarrow', index=False)
    print("Ingestion abgeschlossen.")

if __name__ == "__main__":
    process_pgn()