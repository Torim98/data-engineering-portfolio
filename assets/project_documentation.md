\# Projekt-Dokumentation: Batch-basierte Datenarchitektur für Schach-Analysen



Diese Dokumentation fasst die Ergebnisse der drei Projektphasen (Konzeption, Umsetzung, Finalisierung) zusammen. Diese mussten im Rahmen des Portfolios in drei Feedbackschleifen eingereicht werden.



---



\## Phase 1: Konzeption



Im Rahmen dieses Portfolios wird eine skalierbare Dateninfrastruktur konzipiert und implementiert, die als Backend für eine Machine-Learning-Applikation zur Vorhersage von Turnierausgängen dient. Ziel ist die Prognose von Gewinnwahrscheinlichkeiten basierend auf ELO-Ratings und Eröffnungsstrategien (ECO-Codes).



Als Datenbasis dient die \*\*Lichess Open Database\*\* (database.lichess.org), welche monatlich über 90 Millionen Partien bereitstellt und somit die Anforderung an massive Datenmengen (> 1 Mio. Datensätze) sowie die Verfügbarkeit von Zeitstempeln erfüllt. Die Verarbeitung der semi-strukturierten PGN-Rohdaten (`.zst`-komprimiert) stellt hierbei eine spezifische Data-Engineering-Herausforderung dar.



\*\*Die Architektur folgt einem containerisierten Microservice-Ansatz auf Basis eines Data Lakes:\*\*



1\.  \*\*Data Ingestion:\*\* Ein dedizierter Microservice liest den Datenstrom, dekomprimiert diesen und extrahiert mittels `python-chess` Library relevante Metadaten und Spielergebnisse. Die Speicherung erfolgt im spaltenorientierten \*\*Apache Parquet\*\*-Format („Bronze Layer“), um Speicherplatz durch Kompression (Snappy) zu optimieren und Schema-Integrität zu gewährleisten.

2\.  \*\*Processing \& Aggregation:\*\* Ein nachgelagerter Service bereinigt die Daten (z. B. Filterung nicht gewerteter „Bullet“-Partien) und aggregiert Features für das ML-Modell in einen „Gold Layer“.

3\.  \*\*Visualization:\*\* Ein \*\*Streamlit\*\*-basierter Dashboard-Service greift auf die prozessierten Daten im Gold Layer zu und visualisiert die Ergebnisse (z.B. Gewinnwahrscheinlichkeiten nach Eröffnung) interaktiv. Dies dient der Validierung der Datenqualität und ermöglicht ein exploratives Analysieren der Feature-Relevanz im Rahmen der Data Exploration.



\*\*Qualitätsanforderungen:\*\*



\*   \*\*Reliability:\*\* Alle Ingestion-Prozesse sind idempotent gestaltet (Vermeidung von Duplikaten bei Neustarts).

\*   \*\*Reproduzierbarkeit:\*\* Gewährleistet durch Infrastructure as Code (Docker Compose) und Versionierung im Git-Repository.

\*   \*\*Scalability:\*\* Architektonisch durch Data Partitioning vorbereitet. Da die Microservices stateless konzipiert sind, ermöglicht dies in einem produktiven Szenario eine nahtlose horizontale Skalierung (z.B. via Kubernetes) oder den Wechsel auf Distributed Frameworks wie Apache Spark.

\*   \*\*Data Governance:\*\* Strikte Trennung der Layer und Anonymisierung von Spielernamen (Prinzip der Datensparsamkeit).



\*\*Technologie-Stack:\*\*

Das System basiert auf Docker-Containern mit `python:3.11-slim` sowie `streamlit` für das Frontend. Diese Wahl ermöglicht es, von den signifikanten Performance-Optimierungen neuerer Python-Versionen zu profitieren, während das Slim-Image die Container-Größe minimiert.



---



\## Phase 2: Umsetzung \& Reflexion



Die Implementierung der in Phase 1 konzipierten Architektur erfolgte mittels \*\*Docker Compose\*\*, welches die Orchestrierung der drei Microservices (Ingestion, Processing, Dashboard) steuert. Der Quellcode ist modular in einem Git-Repository organisiert, wobei sensible Daten und Rohdaten durch `.gitignore`-Regeln exkludiert wurden.



\### Technische Herausforderungen \& Lösungen



Während der Entwicklung trat eine \*\*„Race Condition“\*\* auf: Der Processing-Service startete zeitgleich mit dem Ingestion-Service und brach ab, da die benötigten Rohdaten noch nicht vorlagen.



\*   \*\*Lösung:\*\* Um dieses Problem im Sinne der \*\*Reliability\*\* zu lösen, wurde die `docker-compose.yml` um eine explizite Abhängigkeit (`condition: service\_completed\_successfully`) erweitert. Dies stellt sicher, dass nachgelagerte Services erst starten, wenn der vorangegangene Batch-Prozess erfolgreich beendet wurde.



\### Erweiterung der Architektur



Basierend auf dem Feedback aus Phase 1 wurde die Architektur um einen \*\*Dashboard-Microservice\*\* (basierend auf Streamlit) erweitert. Dieser visualisiert die aggregierten Daten aus dem „Gold Layer“ (Parquet) und ermöglicht eine sofortige Validierung der Ergebnisse (z. B. Gewinnverteilung nach Eröffnungen).



\### Sicherstellung der Anforderungen



\*   \*\*Idempotenz:\*\* Die Python-Skripte sind so implementiert, dass sie bei jedem Lauf die Zieldateien im Data Lake vollständig überschreiben. Dies garantiert, dass ein Neustart der Pipeline (z. B. nach einem Fehler) keine Datenduplikate erzeugt.

\*   \*\*Skalierbarkeit (Partitioning):\*\* Um Speicherüberläufe (OOM) bei großen Datensätzen zu vermeiden, implementiert der Ingestion-Service ein \*\*Chunking-Verfahren\*\*. Daten werden nicht komplett im RAM gehalten, sondern in partitionierten Parquet-Dateien (Batch-Größe konfigurierbar via `CHUNK\_SIZE`) in den Data Lake geschrieben. Außerdem nutzt der Prozess das Python `multiprocessing` (`ProcessPoolExecutor`), um mehrere Quelldateien parallel auf allen verfügbaren CPU-Kernen zu verarbeiten.

\*   \*\*Wartbarkeit:\*\* Alle Pfad-Konfigurationen werden über Umgebungsvariablen gesteuert, sodass keine Hard-Coding-Anpassungen im Quellcode notwendig sind.

\*   \*\*Reproduzierbarkeit:\*\* Durch die Definition der Laufzeitumgebung in Dockerfiles (`python:3.11-slim`) und der Abhängigkeiten in `requirements.txt` ist das System auf jedem Rechner mit Docker identisch ausführbar. Konfigurationsparameter sind in dedizierte `.env`-Dateien ausgelagert.



Weitere technische Details können direkt dem \[Git-Repository](https://github.com/Torim98/data-engineering-portfolio) entnommen werden.



---



\## Phase 3: Abstracts



\### Short Abstract

> Dieses Portfolio realisiert eine skalierbare Batch-ETL-Pipeline für Schachdaten (Lichess). Basierend auf Docker-Microservices werden PGN-Dateien parallel ingestiert, bereinigt und als partitionierte Parquet-Dateien gespeichert. Ein Streamlit-Dashboard visualisiert aggregierte Analysen. Die Architektur gewährleistet Idempotenz, Skalierbarkeit und Reproduzierbarkeit durch Infrastructure-as-Code und Python.



\### Full Abstract: Batch-basierte Datenarchitektur für Schach-Analysen



Im Kontext moderner Machine-Learning-Anwendungen stellt die effiziente Bereitstellung und Aufbereitung qualitativ hochwertiger Trainingsdaten eine fundamentale Herausforderung dar. Rohdaten liegen in der Praxis häufig unstrukturiert und in massiven Volumina vor, was eine direkte Nutzung in analytischen Modellen verhindert und vorgelagerte Engineering-Prozesse unabdingbar macht.



Gegenstand dieses Projektes ist die Konzeption und Implementierung einer solchen robusten Data-Engineering-Pipeline am Beispiel der \*\*Lichess Open Database\*\*. Diese Plattform stellt monatlich über 90 Millionen Schachpartien im semi-strukturierten PGN-Format (\*Portable Game Notation\*) bereit. Das primäre Ziel des Portfolios bestand darin, eine skalierbare, batch-basierte Backend-Architektur zu entwickeln, die in der Lage ist, diese historischen Schachdaten performant zu verarbeiten, zu bereinigen und aggregierte Metriken – beispielsweise Gewinnwahrscheinlichkeiten basierend auf Eröffnungsstrategien – für nachgelagerte Analysen bereitzustellen. Neben der technischen Funktionalität lag der Fokus explizit auf der Erfüllung nicht-funktionaler Anforderungen wie Zuverlässigkeit (\*\*Reliability\*\*), Skalierbarkeit (\*\*Scalability\*\*) und Wartbarkeit (\*\*Maintainability\*\*).



Die technische Realisierung erfolgte in Form einer containerisierten Microservice-Architektur, die mittels \*\*Docker\*\* und \*\*Docker Compose\*\* orchestriert wird. Dieser Ansatz gewährleistet durch das Prinzip \*Infrastructure as Code\* eine strikte Isolation der Komponenten sowie die Reproduzierbarkeit der Laufzeitumgebung auf unterschiedlichen Systemen. Architektonisch folgt das System dem Konzept eines lokalen Data Lakes, welcher eine \*\*Medallion Architecture\*\* implementiert, um Rohdaten (Bronze Layer) logisch von veredelten Daten (Gold Layer) zu trennen. Das Gesamtsystem gliedert sich hierbei in drei entkoppelte Services.



Den Einstiegspunkt bildet ein in Python implementierter \*\*Ingestion-Service\*\*, welcher die komprimierten Datenströme (`.pgn.zst`) einliest. Aufgrund der hohen Rechenintensität des Parsings von Schachnotationen wurde dieser Prozess mittels `ProcessPoolExecutor` parallelisiert, um durch Multiprocessing sämtliche verfügbaren CPU-Kerne effizient auszulasten und den \*Global Interpreter Lock\* (GIL) von Python zu umgehen. Die extrahierten Metadaten werden anschließend im spaltenorientierten \*\*Apache Parquet\*\*-Format persistiert, welches aufgrund seiner hohen I/O-Effizienz und Kompressionsraten (Snappy) gegenüber zeilenbasierten Formaten wie CSV bevorzugt wurde.



Nachgelagert übernimmt ein \*\*Processing-Service\*\* die Transformation und Aggregation der Daten. Unter Verwendung der Bibliotheken Pandas und PyArrow werden hierbei invalide Partien gefiltert und statistische Kennzahlen aggregiert. Basierend auf dem Feedback aus der Konzeptionsphase wurde die Architektur zudem iterativ um eine Visualisierungskomponente erweitert. Ein \*\*Streamlit\*\*-basierter Dashboard-Service greift auf die aggregierten Daten des Gold Layers zu und ermöglicht eine explorative Datenanalyse (EDA). Dies dient nicht nur der Präsentation, sondern primär der Validierung der Pipeline-Ergebnisse und der Datenqualität.



Ein wesentlicher Aspekt der Arbeit war die Sicherstellung der Systemstabilität und Skalierbarkeit. Während der Implementierungsphase wurde eine \*\*Race Condition\*\* identifiziert, bei der der Processing-Service startete, bevor die Datenakquise abgeschlossen war. Dieses Problem wurde durch die Implementierung expliziter Abhängigkeitsregeln in der Container-Orchestrierung gelöst, sodass Services nun deterministisch auf den erfolgreichen Abschluss vorgelagerter Prozesse warten. Um Speicherüberläufe (\*Out-of-Memory Errors\*) bei der Verarbeitung großer Datensätze zu vermeiden, wurde zudem ein \*\*Chunking-Verfahren\*\* implementiert. Der Ingestion-Service hält Daten nicht vollständig im Arbeitsspeicher, sondern schreibt diese in partitionierten Batches konstanter Größe auf das Speichermedium. Dies ermöglicht die Verarbeitung theoretisch unbegrenzt großer Datensätze bei konstantem RAM-Verbrauch und demonstriert die horizontale Skalierbarkeit des Systems. Des Weiteren ist die Pipeline vollständig \*\*idempotent\*\* ausgelegt: Vor jedem Ausführungszyklus werden temporäre Zielordner bereinigt und Schreiboperationen erfolgen überschreibend, sodass auch bei Neustarts nach Fehlern keine Datenduplikate entstehen.



Auch Aspekte der \*\*Data Governance\*\* und des Datenschutzes wurden im Design berücksichtigt. Obwohl es sich bei der Lichess-Datenbank um öffentliche Lizenzdaten handelt, werden im Sinne der Datensparsamkeit personenbezogene Merkmale wie Spielernamen bereits während des Ingestion-Prozesses verworfen, da diese für die analytische Fragestellung der Eröffnungsstrategie irrelevant sind. Die Wartbarkeit des Systems wird durch den konsequenten Verzicht auf Hard-Coding sichergestellt: Sämtliche Konfigurationsparameter wie Pfade, Limits oder Parallelisierungsgrade werden über Umgebungsvariablen in die Container injiziert.



Die Durchführung des Projektes ermöglichte eine Vertiefung wesentlicher technischer und methodischer Kompetenzen. Neben dem praktischen Umgang mit Big-Data-Formaten wie Parquet und der Container-Orchestrierung stand insbesondere die Problemlösungskompetenz im Vordergrund. Die Analyse und Behebung der Race Condition sowie die Speicheroptimierung durch Partitionierung erforderten ein tiefgehendes Verständnis der zugrundeliegenden Systemarchitektur. Zudem verdeutlichte die agile Integration des Dashboards den Mehrwert flexibler, modularer Architekturen.



Abschließend ist festzuhalten, dass die entwickelte Batch-Lösung eine unverzichtbare Basis für historische Analysen und das Training komplexer Machine-Learning-Modelle bildet (\*\*Batch Layer\*\*). Um das System jedoch um Echtzeit-Fähigkeiten (\*Unbounded Data\*) zu erweitern, beispielsweise für die Live-Analyse laufender Partien, wäre die Integration eines parallelen Stream-Processing-Pfades im Sinne einer \*\*Lambda-Architektur\*\* zielführend. In diesem Szenario würde der bestehende Batch-Pfad erhalten bleiben, während ein zusätzlicher \*\*Speed Layer\*\* implementiert wird. Dieser würde die Lichess Streaming API anbinden und Events über einen Message Broker wie \*\*Apache Kafka\*\* puffern. Für die Verarbeitung käme statt der Pandas-Logik eine Streaming-Engine wie \*\*Apache Flink\*\* oder Spark Streaming zum Einsatz, welche Windowing-Operationen auf den Echtzeit-Daten durchführt. Die Ergebnisse dieses Speed-Layers würden in einer für geringe Latenzen optimierten Datenbank (z. B. Redis oder InfluxDB) vorgehalten und im Dashboard mit den historischen Batch-Daten zu einer ganzheitlichen Sicht zusammengeführt.

