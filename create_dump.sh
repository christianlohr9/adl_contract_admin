#!/bin/bash

# Definiere den Pfad zur SQLite-Datenbank und den Zielspeicherort für die Dump-Datei
DB_PATH="./data/adl_data.db"
DUMP_PATH="./data/dump.sql"

# Überprüfen, ob die SQLite-Datenbank existiert
if [ ! -f "$DB_PATH" ]; then
  echo "Datenbank $DB_PATH nicht gefunden!"
  exit 1
fi

# Erstellen des Dumps der SQLite-Datenbank
echo "Erstelle Dump der Datenbank..."
sqlite3 "$DB_PATH" .dump > "$DUMP_PATH"

if [ $? -eq 0 ]; then
  echo "Dump erfolgreich erstellt: $DUMP_PATH"
else
  echo "Fehler beim Erstellen des Dumps!"
  exit 1
fi

#chmod +x create_dump.sh # Mache das Skript ausführbar:
#./create_dump.sh # Führe das Skript aus
