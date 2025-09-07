import json
import logging
import os
import re
from typing import Any, Dict, List, Tuple, Optional
import ollama

# =========================
# KONFIGURATION
# =========================
file_path = r"C:\Users\bernh\Documents\Job\Forschung\AI_Complaint\ETQ_Filtered.json"
model_name = "qwen3:8b"

# Der Prompt füllt nur Felder, die leer sind oder "to be filled" enthalten.
# Erhält die übrigen Werte unverändert.
prompt_template = """Du bist ein hilfreiches System. Du erhältst eine Tabellenzeile als JSON.
AUFGABE:
- Fülle NUR die Felder aus, deren Wert leer ist ("" oder null) oder exakt "to be filled" lautet.
- Nutze ausschließlich die vorhandenen Informationen aus der Zeile (keine Halluzinationen).
- Antworte mit einem kompakten JSON-Objekt, das NUR die geänderten/ausgefüllten Schlüssel-Wert-Paare enthält.
- Wenn nichts sinnvoll ausfüllbar ist, antworte mit "{}".
- KEIN Fließtext, KEINE Erklärungen, NUR JSON.

Hier ist die Zeile:
{row_data}
"""

# Logging
log_file = 'processing_detect.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)

# =========================
# HILFSFUNKTIONEN
# =========================
def extract_json_snippet(text: str) -> Optional[str]:
    """
    Versucht, das erste valide JSON-Objekt aus einem Text zu extrahieren.
    """
    if not isinstance(text, str):
        return None
    # Schneller Treffer: beginnt/endet wie JSON
    text = text.strip()
    if text.startswith("{") and text.endswith("}"):
        try:
            json.loads(text)
            return text
        except Exception:
            pass
    # Fallback: suche das erste {...} mit rudimentärem Klammerzähler
    stack = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == "{":
            if stack == 0:
                start = i
            stack += 1
        elif ch == "}":
            if stack > 0:
                stack -= 1
                if stack == 0 and start != -1:
                    candidate = text[start:i+1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except Exception:
                        # weiter suchen
                        start = -1
                        continue
    return None

def find_row_lists(data: Any) -> List[Tuple[List[str], List[Dict[str, Any]], Dict[str, Any]]]:
    """
    Durchsucht rekursiv das geladene JSON-Objekt nach Listen von Zeilen (List[Dict]).
    Gibt eine Liste von Tripeln zurück:
      (pfad, rows_ref, parent_obj)
    - pfad: Liste der Keys, wie man zur rows-Liste kommt (nur zur Info/Logging).
    - rows_ref: die tatsächlich gefundene Referenz auf die rows-Liste (List[Dict]).
    - parent_obj: das Objekt, das das Feld 'rows' enthält (z.B. die Excel-Tabelle), damit wir später ersetzen können.
    Heuristik: Wir suchen Keys namens "rows" deren Wert eine Liste von Dictionaries ist.
    """
    results: List[Tuple[List[str], List[Dict[str, Any]], Dict[str, Any]]] = []

    def _walk(obj: Any, path: List[str]):
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_path = path + [k]
                # Kandidat: rows
                if k.lower() == "rows" and isinstance(v, list) and (len(v) == 0 or isinstance(v[0], dict)):
                    results.append((new_path, v, obj))
                # weiter tiefer suchen
                _walk(v, new_path)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                _walk(item, path + [f"[{idx}]"])

    _walk(data, [])
    return results

def should_fill_value(val: Any) -> bool:
    """
    Entscheidet, ob ein Feld auszufüllen ist.
    - None, "", "to be filled" (case-insensitive, whitespace tolerant)
    """
    if val is None:
        return True
    if isinstance(val, str):
        if val.strip() == "" or val.strip().lower() == "to be filled":
            return True
    return False

def build_minimal_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Liefert ein Subset der Zeile, das nur die potenziell zu füllenden Felder enthält.
    (Optional – der Prompt funktioniert auch mit der vollen Zeile. Das Subset macht es dem Modell leichter.)
    """
    return {k: v for k, v in row.items() if should_fill_value(v)}

def merge_updates_into_row(row: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fügt die vom LLM gelieferten Updates in die Originalzeile ein (in-place).
    """
    for k, v in updates.items():
        # Nur überschreiben, wenn das Feld tatsächlich ausfüllbar war – schützt vor unerwünschten Änderungen
        if k in row and should_fill_value(row[k]):
            row[k] = v
    return row

# =========================
# HAUPTLOGIK
# =========================
def process_file(file_path: str, model_name: str, prompt_template: str):
    logging.info(f"Starte Verarbeitung. Lade Datei: {file_path}")
    if not os.path.exists(file_path):
        print(f"❌ FEHLER: Datei nicht gefunden: {file_path}")
        return

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.info("Datei erfolgreich geladen.")
    except json.JSONDecodeError as e:
        print(f"❌ FEHLER: Ungültiges JSON: {e}")
        return
    except Exception as e:
        print(f"❌ FEHLER beim Lesen: {e}")
        return

    # Finde alle rows-Listen
    row_lists = find_row_lists(data)
    if not row_lists:
        logging.error("Keine rows-Listen gefunden. Prüfe Struktur.")
        print("❌ FEHLER: Konnte keine 'rows' Listen in der JSON-Struktur finden.")
        return

    logging.info(f"{len(row_lists)} rows-Listen gefunden.")
    total_rows = sum(len(rows) for _, rows, _ in row_lists)
    print(f"✅ Struktur erkannt. Gefundene Tabellen: {len(row_lists)} • Gesamtzeilen: {total_rows}")

    processed_count = 0
    skipped_count = 0

    for path, rows, parent in row_lists:
        path_str = " → ".join(path)
        logging.info(f"Verarbeite rows unter Pfad: {path_str} (Anzahl: {len(rows)})")
        print(f"\n🔹 Tabelle: {path_str} (Zeilen: {len(rows)})")

        for i, row in enumerate(rows, start=1):
            # Prüfe, ob überhaupt etwas zu füllen ist
            fill_targets = build_minimal_row(row)
            if not fill_targets:
                skipped_count += 1
                if i % 100 == 0 or i == 1:
                    print(f"  • Zeile {i}: nichts zu füllen — übersprungen")
                continue

            try:
                full_prompt = prompt_template.format(row_data=json.dumps(row, ensure_ascii=False))
                response = ollama.chat(
                    model=model_name,
                    messages=[{"role": "user", "content": full_prompt}],
                    format="json"  # Modell soll JSON liefern
                )
                content = response["message"]["content"]
                snippet = extract_json_snippet(content)
                if not snippet:
                    raise ValueError("Konnte kein valides JSON in der Modellantwort finden.")

                updates = json.loads(snippet)
                if not isinstance(updates, dict):
                    raise ValueError("Modellantwort ist kein JSON-Objekt.")

                merge_updates_into_row(row, updates)
                processed_count += 1

                if i % 50 == 0 or i == 1:
                    print(f"  ✅ Zeile {i}: aktualisiert (Felder: {', '.join(updates.keys()) if updates else '—'})")

            except Exception as e:
                logging.error(f"Fehler in Zeile {i} @ {path_str}: {e}")
                print(f"  ⚠️ Zeile {i}: Fehler bei der Verarbeitung — Original beibehalten. ({e})")
                skipped_count += 1
                continue

    # Ausgabe speichern – gleiche Struktur, nur rows wurden aktualisiert
    directory = os.path.dirname(file_path)
    base = os.path.basename(file_path)
    name, ext = os.path.splitext(base)
    out_path = os.path.join(directory, f"{name}_filled{ext if ext else '.json'}")

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.info(f"Fertig. Ausgabedatei: {out_path}")
        print(f"\n🎉 Fertig! Aktualisierte Datei gespeichert unter:\n{out_path}")
        print(f"   Aktualisierte Zeilen: {processed_count} • Übersprungen/Fehler: {skipped_count}")
    except Exception as e:
        logging.error(f"Ausgabedatei konnte nicht gespeichert werden: {e}")
        print(f"❌ FEHLER beim Speichern: {e}")

if __name__ == "__main__":
    process_file(file_path, model_name, prompt_template)
