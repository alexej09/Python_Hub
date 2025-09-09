#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Enhanced JSON row enricher for Ollama/Mistral (V2 - Refactored)

Reads JSON input (either a flat list of row-objects OR the nested Excel-export
structure: sheets -> <sheet> -> excel_tables[0] -> {headers, rows}).

For each input row, the script:
  1) Calls a local Mistral model via Ollama and asks four focused questions.
     Instead of a naive retry, it parses answers more flexibly.
  2) Adds NEW columns with model-generated entries ONLY for those new fields:
       - "Issue related to breakage?" (Yes/No)
       - "Issue related to ceramic tip?" (Yes/No)
       - "When was the issue detected?" (one of fixed labels)
       - "Type of Patient-Harm?" (one of fixed labels)
       - "Summary" (2–4 sentences, includes verbatim quotes w/ [Column=...])
     Existing/original columns and their headers are copied from the input and
     are NOT produced by the model. Their order is preserved.
  3) Logs one single-sentence per row summarizing the answers and whether a
     response could be generated (logging keywords only: INFO, WARNING, ERROR).

Output is ALWAYS a single JSON object mirroring the input structure (no JSONL).

USAGE EXAMPLE:
    python mistral_ollama_json_enricher_v2.py \
        --input C:/path/TEST.json \
        --out C:/path/Output.json \
        --model mistral:7b-instruct

Requirements:
    - Python 3.9+
    - requests (pip install requests)
    - Ollama running locally (default http://localhost:11434) with a Mistral
      instruct model available, e.g. `ollama pull mistral:7b-instruct`
"""

from __future__ import annotations

# =============================
# >>> USER-CONFIGURABLE SETTINGS (CLEARLY MARKED) <<<
# Adjust these to your needs. Keep model temperature very low for repeatability.
# =============================
DEFAULT_INPUT_PATH = r"C:\Users\Alex.bernhard\OneDrive - Olympus\Organisatorisches\KI_Projekte\Complaint_Analysis\TEST.json"
DEFAULT_OUTPUT_PATH = r"C:\Users\Alex.bernhard\OneDrive - Olympus\Organisatorisches\KI_Projekte\Complaint_Analysis\Output.json"

# Ollama / Model settings
OLLAMA_HOST = "http://localhost:11434"
OLLAMA_MODEL = "mistral:7b-instruct"   # e.g., "mistral", "mistral:7b-instruct"
TEMPERATURE = 0.0                      # very low for reproducibility
NUM_PREDICT = 128                      # per question; raise if answers get cut
TIMEOUT_SEC = 120.0
REQUEST_DELAY_SEC = 1.0                # optional sleep between requests for each ROW

# Label sets (kept strict to force exact labels & quotes)
STRICT_YESNO = ("Yes", "No")
DETECTED_LABELS = (
    "During Inspection",
    "During Operation",
    "During Reprocessing",
    "During Service Activities",
    "During follow-up Examination of Patient",
)
PATIENT_HARM_LABELS = (
    "Bleeding or other severe damage",
    "Delay of Surgery",
    "none",
)

# The NEW columns to be added to each row (keys must match desired headers):
NEW_COLUMNS = [
    "Issue related to breakage?",
    "Issue related to ceramic tip?",
    "When was the issue detected?",
    "Type of Patient-Harm?",
    "Summary",
]
# =============================

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from collections import OrderedDict

import requests

# -----------------------------
# Logging: only plain keywords INFO/WARNING/ERROR, single-sentence per row.
# -----------------------------
logger = logging.getLogger("enricher")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# -----------------------------
# Input loader: supports flat list or nested Excel-export structure.
# -----------------------------

def load_rows_from_json(json_path: Path) -> Tuple[List[Dict[str, Any]], Optional[List[str]], Any]:
    """Loads rows and headers from JSON, returning the original data structure as well."""
    try:
        original_data = json.loads(json_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.error(f"Input file not found: {json_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in input file: {e}")
        sys.exit(1)

    rows: List[Dict[str, Any]] = []
    headers: Optional[List[str]] = None

    # Case A: direct list of objects
    if isinstance(original_data, list):
        if original_data and isinstance(original_data[0], dict):
            rows = original_data
            headers = list(rows[0].keys()) # Preserve original header order
            return rows, headers, original_data
        raise ValueError("JSON list found, but does not contain objects.")

    # Case B: nested Excel structure
    if not isinstance(original_data, dict):
        raise ValueError("Unexpected JSON: neither dict nor list.")

    sheets = original_data.get("sheets", {})
    if sheets:
        if len(sheets) > 1:
            logger.warning(f"Found {len(sheets)} sheets; only processing the first one.")
        
        first_sheet = next(iter(sheets.values()), None)
        if not first_sheet:
            raise ValueError("Found 'sheets' but it is empty.")

        tables = first_sheet.get("excel_tables", [])
        if not tables:
            raise ValueError("Missing 'excel_tables' in first sheet.")
        
        table0 = tables[0]
        rows = table0.get("rows", [])
        headers = table0.get("headers", None)
        
        if not isinstance(rows, list) or not rows:
            raise ValueError("No rows found in 'rows'.")
        
        # Fallback to infer headers from the first row if not explicitly provided
        if headers is None and isinstance(rows[0], dict):
            headers = list(rows[0].keys())
        
        return rows, headers, original_data

    raise ValueError("Cannot detect supported structure (no list and no 'sheets').")

# -----------------------------
# Ollama chat helper
# -----------------------------

def call_ollama_chat(
    model: str,
    messages: List[Dict[str, str]],
    host: str = OLLAMA_HOST,
    temperature: float = TEMPERATURE,
    num_predict: int = NUM_PREDICT,
    timeout: float = TIMEOUT_SEC,
) -> Optional[str]:
    """Calls the Ollama chat API and returns the content or None on error."""
    url = f"{host.rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {"temperature": temperature, "num_predict": num_predict},
    }
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        content = data.get("message", {}).get("content", "")
        return (content or "").strip()
    except requests.exceptions.RequestException as e:
        logger.error(f"Ollama API request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during API call: {e}")
        return None

# -----------------------------
# System and User Prompts
# -----------------------------

SYSTEM_PROMPT = (
    "You are a precise, bilingual data assistant working on single table rows. "
    "Only output what is requested. Keep outputs minimal, English-only, and deterministic."
)

def prompt_yesno_breakage(row_json: str) -> str:
    return f"""Task: Decide if the issue is related to breakage based on the row content.
Allowed answers: Yes or No. Output exactly one word: Yes or No.
Evidence is in this JSON row. Consider fields such as 'H6 Medical Device Problem Code', 'Event Description', 'Evaluation Result', 'Investigation Conclusion' and related text.
Row JSON:
{row_json}"""

def prompt_yesno_ceramic(row_json: str) -> str:
    return f"""Task: Decide if the issue is related to the ceramic tip based on the row content.
Search for explicit mentions and close synonyms like ceramic tip, ceramic beak, tip, beak, or phrasing indicating ceramic-part damage.
Allowed answers: Yes or No. Output exactly one word: Yes or No.
Row JSON:
{row_json}"""

def prompt_detected_when(row_json: str) -> str:
    labels = " | ".join(DETECTED_LABELS)
    return f"""Task: Classify WHEN the issue was detected based on the row content.
Choose exactly ONE of these labels and output it verbatim (no extra text):
{labels}
Hints: before the operation -> During Inspection; intraoperative -> During Operation; found during cleaning/sterilization -> During Reprocessing; service scenarios -> During Service Activities; later follow-up of the patient -> During follow-up Examination of Patient.
Row JSON:
{row_json}"""

def prompt_patient_harm(row_json: str) -> str:
    labels = " | ".join(PATIENT_HARM_LABELS)
    return f"""Task: Classify the type of patient harm based on the row content.
Choose exactly ONE of these labels and output it verbatim (no extra text):
{labels}
Instructions:
1.  **Actively search for evidence** of patient harm. Focus your analysis on text fields like 'Event Description', 'H6 Health Effect Impact Code', and 'H6 Health Effect Clinical Code'.
2.  Look for explicit keywords and descriptions indicating injury, such as "bleeding", "laceration", "complication", "adverse event", "extended surgery", or "unintended tissue damage".
If there is no indication of patient injury, choose 'none'.
Row JSON:
{row_json}"""

def prompt_summary(row_json: str, ans_breakage: str, ans_ceramic: str, ans_detected: str, ans_harm: str) -> str:
    return f"""Write a concise English summary (2–4 sentences) JUSTIFYING the four answers.
Requirements:
- Quote short, verbatim snippets from the row using double quotes, and after each quote add a source marker like [Column=Event Description].
- Explicitly mention WHICH column each piece of evidence was found in.
- When relevant fields are empty/unknown, state 'not specified'.
- Focus only on evidence supporting: breakage, ceramic tip relation, detection timing, patient harm.
- Output plain text (no markdown).
Given answers: breakage={ans_breakage}; ceramic_tip={ans_ceramic}; detected={ans_detected}; harm={ans_harm}.
Row JSON:
{row_json}"""

# -----------------------------
# Helper Functions
# -----------------------------

def reorder_row(row: Dict[str, Any], headers_list: List[str]) -> OrderedDict:
    """Return row as OrderedDict with keys exactly in headers_list order."""
    return OrderedDict((h, row.get(h, "")) for h in headers_list)

def _parse_flexible_answer(raw_answer: Optional[str], valid_labels: Tuple[str, ...]) -> Optional[str]:
    """Finds the first valid label in a raw string, case-insensitive."""
    if not raw_answer:
        return None
    
    # First, try for an exact match (most common case)
    if raw_answer in valid_labels:
        return raw_answer
        
    # If no exact match, search for the label within the string
    lower_answer = raw_answer.lower()
    for label in valid_labels:
        if label.lower() in lower_answer:
            return label # Return the original, correctly-cased label
    return None

def _ask_and_validate_question(
    prompt_func, *args, 
    valid_labels: Optional[Tuple[str, ...]] = None,
    is_summary: bool = False
) -> Tuple[str, bool]:
    """Helper to ask a question, parse the answer, and return value and validity."""
    question_text = prompt_func(*args)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": question_text}]
    
    raw_answer = call_ollama_chat(
        model=cli_args.model, messages=messages, host=cli_args.host,
        temperature=cli_args.temperature, num_predict=cli_args.num_predict, timeout=cli_args.timeout
    )

    if raw_answer is None: # API call failed
        return "", False

    if is_summary:
        return raw_answer, bool(raw_answer)

    # For classification questions, parse flexibly
    parsed_answer = _parse_flexible_answer(raw_answer, valid_labels)
    if parsed_answer:
        return parsed_answer, True
    else:
        logger.warning(f"Could not parse valid label from model output: '{raw_answer}'")
        return raw_answer, False

# -----------------------------
# Main processing
# -----------------------------

cli_args: argparse.Namespace = None # Global placeholder for command-line arguments

def main() -> None:
    global cli_args
    ap = argparse.ArgumentParser(description="Enrich JSON rows with model-derived fields using Ollama/Mistral.")
    ap.add_argument("--input", "-i", type=Path, default=Path(DEFAULT_INPUT_PATH), help="Path to input JSON file")
    ap.add_argument("--out", "-o", type=Path, default=Path(DEFAULT_OUTPUT_PATH), help="Path to output JSON")
    ap.add_argument("--model", "-m", type=str, default=OLLAMA_MODEL, help="Ollama model name")
    ap.add_argument("--host", type=str, default=OLLAMA_HOST, help="Ollama host, e.g. http://localhost:11434")
    ap.add_argument("--temperature", type=float, default=TEMPERATURE, help="Sampling temperature (keep low)")
    ap.add_argument("--num_predict", type=int, default=NUM_PREDICT, help="Max tokens per answer")
    ap.add_argument("--timeout", type=float, default=TIMEOUT_SEC, help="HTTP timeout seconds")
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY_SEC, help="Optional delay between processed rows")
    cli_args = ap.parse_args()

    try:
        rows, headers, original_data = load_rows_from_json(cli_args.input)
    except ValueError as e:
        logger.error(f"Failed to load or parse input file: {e}")
        sys.exit(1)

    logger.info(f"Loaded {len(rows)} rows to process.")
    enriched_rows: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows, start=1):
        row_json = json.dumps(row, ensure_ascii=False, indent=2)

        ans_breakage, valid_breakage = _ask_and_validate_question(prompt_yesno_breakage, row_json, valid_labels=STRICT_YESNO)
        ans_ceramic, valid_ceramic = _ask_and_validate_question(prompt_yesno_ceramic, row_json, valid_labels=STRICT_YESNO)
        ans_detected, valid_detected = _ask_and_validate_question(prompt_detected_when, row_json, valid_labels=DETECTED_LABELS)
        ans_harm, valid_harm = _ask_and_validate_question(prompt_patient_harm, row_json, valid_labels=PATIENT_HARM_LABELS)
        
        # Pass the (potentially invalid) answers to summary to give it context
        summary_text, valid_summary = _ask_and_validate_question(
            prompt_summary, row_json, ans_breakage, ans_ceramic, ans_detected, ans_harm, is_summary=True
        )

        out_row = dict(row)
        out_row["Issue related to breakage?"] = ans_breakage if valid_breakage else ""
        out_row["Issue related to ceramic tip?"] = ans_ceramic if valid_ceramic else ""
        out_row["When was the issue detected?"] = ans_detected if valid_detected else ""
        out_row["Type of Patient-Harm?"] = ans_harm if valid_harm else ""
        out_row["Summary"] = summary_text if valid_summary else ""
        enriched_rows.append(out_row)

        status_parts = [
            f"breakage={'ok' if valid_breakage else 'fail'}",
            f"ceramic={'ok' if valid_ceramic else 'fail'}",
            f"detected={'ok' if valid_detected else 'fail'}",
            f"harm={'ok' if valid_harm else 'fail'}",
            f"summary={'ok' if valid_summary else 'fail'}",
        ]
        all_valid = all([valid_breakage, valid_ceramic, valid_detected, valid_harm, valid_summary])
        logger.log(logging.INFO if all_valid else logging.WARNING, f"Row {idx}: " + ", ".join(status_parts))

        if cli_args.delay > 0:
            time.sleep(cli_args.delay)

    # --- Preserve original outer structure and write output ---
    output_obj: Any
    if isinstance(original_data, dict) and "sheets" in original_data:
        output_obj = original_data
        first_sheet_key = next(iter(output_obj["sheets"].keys()))
        table0 = output_obj["sheets"][first_sheet_key]["excel_tables"][0]
        
        headers_list = table0.get("headers") or (list(headers) if headers else [])
        for col in NEW_COLUMNS:
            if col not in headers_list:
                headers_list.append(col)
        table0["headers"] = headers_list
        table0["rows"] = [reorder_row(r, headers_list) for r in enriched_rows]

    elif isinstance(original_data, list):
        # BUG FIX: Use the original headers, not inferred ones from the enriched row
        final_headers = (headers or []) + [col for col in NEW_COLUMNS if col not in (headers or [])]
        output_obj = [reorder_row(r, final_headers) for r in enriched_rows]
    else:
        output_obj = enriched_rows # Fallback

    try:
        Path(cli_args.out).write_text(
            json.dumps(output_obj, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"Wrote output with preserved structure to {cli_args.out}")
    except Exception as e:
        logger.error(f"Failed to write output file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Aborted by user")
        sys.exit(130)
