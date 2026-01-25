import chess.pgn
import zstandard as zstd
import pandas as pd
import io
import os
import glob
import shutil

# Konfiguration aus Environment Variables (oder Standardwerte)
SOURCE_DIR = os.getenv("SOURCE_DIR", "/data")
FILE_PATTERN = os.getenv("FILE_PATTERN", ".pgn.zst")
TARGET_DIR = os.getenv("TARGET_DIR", "/data/raw")
# Test-Limit laden (Standard: 10.000, bei 0 oder -1 -> kein Limit)
MAX_GAMES = int(os.getenv("MAX_GAMES", 10000))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 10000))

def get_files():
    # Sucht alle Dateien im Ordner, die auf .pgn.zst enden
    search_path = os.path.join(SOURCE_DIR, f"*{FILE_PATTERN}")
    files = glob.glob(search_path)
    files.sort() # Sortieren für reproduzierbare Reihenfolge
    return files

def save_chunk(data, chunk_id):
    if not data:
        return
    
    filename = f"part_{chunk_id:05d}.parquet" # z.B. part_00001.parquet
    filepath = os.path.join(TARGET_DIR, filename)
    
    # Als Parquet speichern
    print(f"   -> Speichere Chunk {chunk_id} ({len(data)} Zeilen) nach {filename}...")
    df = pd.DataFrame(data)
    
    # Data Cleaning - Datentypen anpassen (Elo zu Int)
    df['WhiteElo'] = pd.to_numeric(df['WhiteElo'], errors='coerce').fillna(0).astype(int)
    df['BlackElo'] = pd.to_numeric(df['BlackElo'], errors='coerce').fillna(0).astype(int)
    
    df.to_parquet(filepath, engine='pyarrow', index=False)
def process_pgn():
    files = get_files()
    if not files:
        print(f"WARNUNG: Keine Dateien mit Endung '{FILE_PATTERN}' in {SOURCE_DIR} gefunden.")
        return

    # Idempotenz: Zielordner bereinigen (alte Chunks löschen), damit wir sauber starten
    if os.path.exists(TARGET_DIR):
        print(f"Bereinige Zielordner {TARGET_DIR}...")
        shutil.rmtree(TARGET_DIR)
    os.makedirs(TARGET_DIR, exist_ok=True)
    
    current_chunk_data = []
    chunk_counter = 0
    total_games_processed = 0

    # Äußere Schleife: Über alle Dateien iterieren
    for file_path in files:
        # Abbruchbedingung prüfen (wenn Limit erreicht)
        if MAX_GAMES > 0 and total_games_processed >= MAX_GAMES:
            break

        print(f"--> Verarbeite Datei: {os.path.basename(file_path)}")
        
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
                        
                        current_chunk_data.append(row)
                        total_games_processed += 1

                        # CHECK: Ist der Chunk voll?
                        if len(current_chunk_data) >= CHUNK_SIZE:
                            save_chunk(current_chunk_data, chunk_counter)
                            current_chunk_data = [] # Speicher leeren!
                            chunk_counter += 1
                            print(f"Gesamtfortschritt: {total_games_processed} Partien...")

        except Exception as e:
            print(f"Fehler beim Lesen der Datei {file_path}: {e}")

    # Den "Rest" speichern (Puffer)
    if current_chunk_data:
        save_chunk(current_chunk_data, chunk_counter)

    print(f"Ingestion beendet. {total_games_processed} Partien in {chunk_counter + 1} Files gespeichert.")

if __name__ == "__main__":
    process_pgn()