# Data Engineering Portfolio: Batch-Processing Pipeline für Schach-Analysen

Dieses Repository beinhaltet die Implementierung einer **Batch-basierten Datenarchitektur** im Rahmen des Portfolioprojekts "Data Engineering". Ziel des Projekts ist der Aufbau einer containerisierten Pipeline, die Schachpartien (PGN-Format) einliest, bereinigt, aggregiert und in einem Dashboard visualisiert.

Als Datenbasis dient die [Lichess Open Database](https://database.lichess.org/).

## 🏗️ Architektur

Die Pipeline folgt einem Microservice-Ansatz und ist vollständig in **Docker** containerisiert. Sie implementiert eine "Medallion Architecture" (Bronze/Gold Layer) innerhalb eines lokalen Data Lakes.

**Die Komponenten:**

1.  **Ingestion Service:**
    *   Liest komprimierte Rohdaten (`.pgn.zst`).
    *   Extrahiert Metadaten (Elo, Eröffnung, Ergebnis) mittels `python-chess`.
    *   Speichert Rohdaten als partitionierte **Parquet**-Dateien (Bronze Layer).
2.  **Processing Service:**
    *   Wartet auf Abschluss der Ingestion.
    *   Führt Data Cleaning durch (Filterung ungültiger Partien).
    *   Aggregiert Gewinnwahrscheinlichkeiten basierend auf Eröffnungen (ECO-Codes).
    *   Speichert Ergebnisse im Gold Layer.
3.  **Dashboard Service:**
    *   Visualisiert die Ergebnisse mittels **Streamlit**.
    *   Ermöglicht explorative Datenanalyse im Browser.

---

## 🚀 Technologie-Stack

*   **Infrastructure as Code:** Docker & Docker Compose
*   **Sprache:** Python 3.11 (Slim Images)
*   **Datenformat:** Apache Parquet (Snappy Compression)
*   **Libraries:** Pandas, PyArrow, Python-Chess, Zstandard
*   **Frontend:** Streamlit

---

## 🛠️ Installation & Ausführung

Voraussetzung: **Docker Desktop** muss installiert sein und laufen.

### 1. Repository klonen
```bash
git clone https://github.com/Torim98/data-engineering-portfolio.git
cd data-engineering-portfolio
```

### 2. Testdaten herunterladen

Aus Gründen der Speicherplatzoptimierung sind die Rohdaten nicht im Repository enthalten.

1. Lade eine Beispieldatei von database.lichess.org herunter (für Tests empfehlen sich kleinere Dateien von 2013/2014).
2. Platziere die Datein im Ordner `data/`.

Die Ordnerstruktur sollte so aussehen:

```Text
/portfolio-chess-analytics
  ├── data/
  │   └── lichess_sample1.pgn.zst
  │   └── lichess_sample2.pgn.zst
  ├── ingestion/
  ├── processing/
  ├── dashboard/
  └── docker-compose.yml
```

### 3. Pipeline starten

Führe folgenden Befehl im Hauptverzeichnis aus:

```Bash
docker compose up --build
```

### 4. Dashboard öffnen

Sobald die Pipeline durchgelaufen ist, ist das Dashboard unter folgender URL erreichbar:

👉 http://localhost:8501

![Dashboard Vorschau](assets/dashboard_preview.png)

---

## 💡 Engineering-Konzepte

*   **Idempotenz**: Die Pipeline ist so konzipiert, dass sie beliebig oft neu gestartet werden kann. Zieldateien werden überschrieben, sodass keine Duplikate entstehen.
*   **Reliability**: Durch `service_completed_successfully` Conditions in Docker Compose wird sichergestellt, dass Services in der korrekten Reihenfolge starten (Vermeidung von Race Conditions).
*   **Reproduzierbarkeit**: Alle Abhängigkeiten sind in `requirements.txt` fixiert und laufen in isolierten Containern.
*   **Datenschutz**: Spielernamen werden während der Ingestion verworfen (Datensparsamkeit).

---

## 📂 Projektstruktur

*   `/ingestion`: Code für den ETL-Prozess (PGN -> Parquet).
*   `/processing`: Code für Aggregation und Feature Engineering.
*   `/dashboard`: Streamlit-Applikation.
*   `/data`: Lokaler Mount für den Data Lake (wird via .gitignore exkludiert).
