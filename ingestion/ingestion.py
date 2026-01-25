import chess.pgn
import zstandard as zstd
import pandas as pd
import io
import os
import glob
import shutil
import logging
import concurrent.futures
import time

# LOGGING SETUP (Dual: Datei + Konsole)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/logs/ingestion.log"), # Schreibt in Datei
        logging.StreamHandler()                     # Schreibt in Konsole (Docker logs)
    ]
)

# Konfiguration aus Environment Variables (oder Standardwerte)
SOURCE_DIR = os.getenv("SOURCE_DIR", "/data")
FILE_PATTERN = os.getenv("FILE_PATTERN", ".pgn.zst")
TARGET_DIR = os.getenv("TARGET_DIR", "/data/raw")
# Test-Limit laden (Standard: 10.000, bei 0 oder -1 -> kein Limit)
MAX_GAMES = int(os.getenv("MAX_GAMES", 10000))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 10000))
# Wenn MAX_WORKERS nicht gesetzt ist, nutze Anzahl der CPUs
MAX_WORKERS = int(os.getenv("MAX_WORKERS", os.cpu_count())) 

def get_files():
    # Sucht alle Dateien im Ordner, die auf .pgn.zst enden
    search_path = os.path.join(SOURCE_DIR, f"*{FILE_PATTERN}")
    files = glob.glob(search_path)
    files.sort() # Sortieren für reproduzierbare Reihenfolge
    return files

def save_chunk(data, source_filename, chunk_id):
    if not data:
        return
    
    # WICHTIG: Dateiname enthält jetzt den Quell-Dateinamen, um Kollisionen zu vermeiden!
    # z.B. part_lichess_2013_00001.parquet
    clean_source_name = os.path.splitext(os.path.basename(source_filename))[0].replace('.', '_')
    filename = f"part_{clean_source_name}_{chunk_id:05d}.parquet"
    filepath = os.path.join(TARGET_DIR, filename)
    
    # Als Parquet speichern
    logging.info(f"Speichere Chunk {chunk_id} von {source_filename} ({len(data)} Partien)...")
    df = pd.DataFrame(data)
    
    # Data Cleaning - Datentypen anpassen (Elo zu Int)
    df['WhiteElo'] = pd.to_numeric(df['WhiteElo'], errors='coerce').fillna(0).astype(int)
    df['BlackElo'] = pd.to_numeric(df['BlackElo'], errors='coerce').fillna(0).astype(int)
    
    df.to_parquet(filepath, engine='pyarrow', index=False)
    
def process_single_file(file_path):
    """
    Diese Funktion läuft isoliert in einem eigenen Prozess für EINE Datei.
    """
    
    logging.info(f"--> Starte Ingestion von: {os.path.basename(file_path)}")
    
    current_chunk_data = []
    chunk_counter = 0
    total_games_processed = 0

    try:
        # Stream öffnen (zstd dekomprimieren)
        with open(file_path, 'rb') as fh:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(fh) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                
                while True:
                    # Auch hier Abbruch prüfen
                    try:
                        game = chess.pgn.read_game(text_stream)
                    except Exception:
                        continue 

                    if game is None: # Ende dieser Datei
                        break 

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
                        save_chunk(current_chunk_data, file_path, chunk_counter)
                        current_chunk_data = [] # Speicher leeren!
                        chunk_counter += 1
                        
                        # Logging
                        if total_games_processed % (CHUNK_SIZE * 5) == 0:
                            logging.info(f"Fortschritt {os.path.basename(file_path)}: {total_games_processed} Games")

    except Exception as e:
        logging.error(f"Fehler beim Lesen der Datei {file_path}: {e}")

    # Rest speichern
    if current_chunk_data:
        save_chunk(current_chunk_data, file_path, chunk_counter)

    logging.info(f"Ingestion von {os.path.basename(file_path)} beendet. {total_games_processed} Partien.")
    return total_games_processed

def main():
    files = get_files()
    if not files:
        logging.warning("Keine Dateien gefunden.")
        return

    # Idempotenz: Zielordner bereinigen (alte Chunks löschen), damit wir sauber starten
    if os.path.exists(TARGET_DIR):
        logging.info(f"Bereinige Zielordner {TARGET_DIR}...")
        try:
            shutil.rmtree(TARGET_DIR)
        except Exception as e:
            logging.warning(f"Konnte Ordner nicht löschen (evtl. Zugriffsproblem): {e}")
    
    os.makedirs(TARGET_DIR, exist_ok=True)

    logging.info(f"Starte Parallelverarbeitung mit {MAX_WORKERS} Workern für {len(files)} Dateien.")
    start_time = time.time()

    # Parallelisierung:
    total_games_all_files = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # map führt die Funktion process_single_file für jeden Eintrag in files aus
        results = executor.map(process_single_file, files)
        
        # Ergebnisse einsammeln
        for count in results:
            total_games_all_files += count

    duration = time.time() - start_time
    logging.info(f"Ingestion abgeschlossen. {total_games_all_files} Partien in {duration:.2f} Sekunden verarbeitet.")

if __name__ == "__main__":
    main()