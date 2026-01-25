import streamlit as st
import pandas as pd
import os
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/logs/dashboard.log"),
        logging.StreamHandler()
    ]
)

DATA_FILE = "/data/processed/stats.parquet"

st.set_page_config(page_title="Chess Analytics", layout="wide")

st.title("♟️ Schach Analyse Dashboard")
st.markdown("Analyse basierend auf der Lichess Open Database (Batch Processing)")

# Funktion zum Laden der Daten mit Cache
def load_data():
    if os.path.exists(DATA_FILE):
        logging.info("Daten erfolgreich für Dashboard geladen.")
        return pd.read_parquet(DATA_FILE)
    logging.warning("Dashboard konnte keine Daten finden.")
    return None

# Warte-Schleife, falls Processing noch läuft
df = load_data()
if df is None:
    st.warning("Daten werden noch verarbeitet... Bitte warten.")
    if st.button("Erneut prüfen"):
        st.rerun()
else:
    # Metriken oben
    col1, col2, col3 = st.columns(3)
    col1.metric("Anzahl Eröffnungen", len(df))
    col2.metric("Meistgespielte Eröffnung", df.iloc[0]['ECO'])
    col3.metric("Höchste Gewinnrate (Top 10)", f"{df.head(10)['WhiteWinRate'].max():.1%}")

    st.divider()

    # Charts
    st.subheader("Top 20 Eröffnungen nach Häufigkeit")
    st.bar_chart(df.head(20).set_index('ECO')['TotalGames'])

    st.subheader("Gewinnrate für Weiß (bei >10 Spielen)")
    st.scatter_chart(df.head(50), x='TotalGames', y='WhiteWinRate', color='ECO')

    # Rohdaten Ansicht
    with st.expander("Zeige Datentabelle"):
        st.dataframe(df)