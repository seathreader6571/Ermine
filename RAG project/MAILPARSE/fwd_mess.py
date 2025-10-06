#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from tqdm import tqdm
import pandas as pd


# -----------------------------
# CONFIG
# -----------------------------
INPUT_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\output_txt\Test batch")
OUTPUT_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\output_txt\FWD_emails")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MAX_DEPTH = 15

# forward section markers
FORWARD_MARKERS = (
    "Begin forwarded message",
    "Forwarded message",
    "Original Message",
)

# localized label → canonical key
LABEL_TO_CANON = {
    "from": "from", "van": "from",
    "to": "to", "aan": "to",
    "cc": "cc",
    "bcc": "bcc",
    "subject": "subject", "onderwerp": "subject",
    "date": "date", "datum": "date", "sent": "date", "verzonden": "date",
}

# single header line regex
HEADER_RE = re.compile(
    r"(?im)^(From|Van|To|Aan|Cc|Bcc|Subject|Onderwerp|Date|Datum|Sent|Verzonden)\s*:\s*(.*)$"
)

# ==========================================
# HELPERS
# ==========================================

def normalize(text: str) -> str:
    """Normalize newlines and strip odd characters."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n")
    text = text.replace("\uFEFF", "")  # remove BOM/zero-width
    return text

def find_forward_start(txt: str) -> int:
    """Find explicit forward marker or first From:/Van: at line start."""
    m = re.search(r"(?im)^(?:>+\s*)?(?:" + "|".join(re.escape(m) for m in FORWARD_MARKERS) + r")", txt)
    if m:
        return m.start()
    m = re.search(r"(?im)^(?:>+\s*)?(From|Van)\s*:", txt)
    return m.start() if m else -1

def parse_header_block(segment: str):
    """
    Parse contiguous header block that starts with From/Van.
    Supports multiline values (continuations).
    Returns (headers_dict or None, remainder_after_headers).
    """
    lines = segment.split("\n")
    headers = {k: None for k in ["from","to","cc","bcc","subject","date"]}
    i, n = 0, len(lines)
    seen_any = False
    current = None

    while i < n:
        line = lines[i]
        if not line.strip():  # blank line marks end of headers
            i += 1
            break
        m = HEADER_RE.match(line)
        if m:
            label = m.group(1).lower()
            value = m.group(2).strip()
            canon = LABEL_TO_CANON.get(label, None)
            if canon:
                headers[canon] = value
                current = canon
                seen_any = True
            i += 1
            # collect continuation lines (value on next line(s))
            while i < n and lines[i].strip() and not HEADER_RE.match(lines[i]):
                cont = lines[i].strip()
                headers[current] = (headers[current] or "") + " " + cont
                i += 1
            continue
        else:
            break
    remainder = "\n".join(lines[i:]).lstrip("\n")
    return (headers if seen_any else None), remainder

def parse_forward_chain(body: str, depth: int = 0):
    """Recursively split email body into main text + forwarded sections."""
    body = normalize(body)
    if depth > MAX_DEPTH or not body.strip():
        return None, []

    idx = find_forward_start(body)
    if idx == -1:
        msg = {"depth": depth, "body": body.strip()}
        return msg, [msg]

    main_text = body[:idx].strip()
    fwd_segment = body[idx:].strip()

    headers, rest = parse_header_block(fwd_segment)
    if headers is None:
        # likely a false positive “From:” inside text
        msg = {"depth": depth, "body": body.strip()}
        return msg, [msg]

    msg = {"depth": depth, "body": main_text}
    msg.update(headers)

    fwd_msg, flat = parse_forward_chain(rest, depth + 1)
    msg["fwd"] = fwd_msg
    return msg, [msg] + flat

# ==========================================
# IO + BATCH
# ==========================================

def process_file(path: Path):
    """Load one JSON email, parse forwards, save structured result."""
    try:
        data = json.load(path.open("r", encoding="utf-8"))
    except Exception:
        print(f"⚠️  Invalid JSON: {path.name}")
        return

    body = data.get("body", "")
    if not isinstance(body, str):
        print(f"⚠️  Skipped {path.name}: no valid 'body' string.")
        return

    nested, flat = parse_forward_chain(body)

    structured = {
        "source_file": path.name,
        "nested_structure": nested,
        "flattened_messages": flat,
    }

    out_path = OUTPUT_DIR / path.name
    json.dump(structured, out_path.open("w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print(f"✅  Saved → {out_path.name}")

def batch_process():
    files = list(INPUT_DIR.glob("*.json"))
    if not files:
        print("No .json files found in input folder.")
        return
    for f in tqdm(files, desc="Parsing emails"):
        process_file(f)

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    batch_process()