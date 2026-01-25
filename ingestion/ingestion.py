import chess.pgn
import zstandard as zstd
import pandas as pd
import io
import os

# Konfiguration aus Environment Variables (oder Standardwerte)
SOURCE_FILE = os.getenv("SOURCE_FILE", "/data/lichess_sample.pgn.zst")
TARGET_FILE = os.getenv("TARGET_FILE", "/data/raw_games.parquet")

def process_pgn():
    print(f"Starte Ingestion von {SOURCE_FILE}...")
    
    data = []
    game_count = 0
    max_games = 5000  # Zum Testen begrenzen

    # Stream öffnen (zstd dekomprimieren)
    with open(SOURCE_FILE, 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            
            while game_count < max_games:
                game = chess.pgn.read_game(text_stream)
                if game is None:
                    break # Ende der Datei

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
                game_count += 1
                
                if game_count % 1000 == 0:
                    print(f"{game_count} Partien verarbeitet...")

    # Als Parquet speichern
    print("Erstelle DataFrame...")
    df = pd.DataFrame(data)
    
    # Datentypen anpassen (Elo zu Int)
    df['WhiteElo'] = pd.to_numeric(df['WhiteElo'], errors='coerce').fillna(0).astype(int)
    df['BlackElo'] = pd.to_numeric(df['BlackElo'], errors='coerce').fillna(0).astype(int)

    print(f"Speichere {len(df)} Zeilen nach {TARGET_FILE}...")
    # Ordner erstellen, falls er nicht existiert
    os.makedirs(os.path.dirname(TARGET_FILE), exist_ok=True) 
    df.to_parquet(TARGET_FILE, engine='pyarrow', index=False)

if __name__ == "__main__":
    process_pgn()