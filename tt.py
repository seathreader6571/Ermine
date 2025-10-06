#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from tqdm import tqdm


INPUT_DIR = Path(r"J:\Ermine\mywritingpad@proton.me\output_txt\Test batch")
OUTPUT_DIR = Path(r"J:\Ermine\mywritingpad@proton.me\output_txt\FWD_emails")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Forwarded message detection (cluster + fallback + safe recursion)
# -----------------------------
HEADER_KEYWORDS = [
    # English
    "From:", "To:", "Cc:", "Date:", "Subject:", "Sent:",
    # Dutch (optional; comment out if not needed)
    "Van:", "Aan:", "Onderwerp:", "Datum:", "Verzonden:"
]

# Map localized/variant header names to canonical keys
HEADER_ALIASES = {
    "from": "from", "van": "from",
    "To": "to", "aan": "to",
    "cc": "cc",
    "bcc": "bcc",  # keep if you want to capture Bcc too
    "subject": "subject", "onderwerp": "subject",
    "date": "date", "datum": "date", "sent": "date", "verzonden": "date",
}

EXPLICIT_FORWARD_MARKERS = [
    "Forwarded message",
    "Original Message",
    "Begin forwarded message",
]

MAX_DEPTH = 15  # safety guard against pathological nesting


#Checks for present marker from EXPLICIT_FORWARD_MARKERS
def is_forward_marker(line: str) -> bool:
    return any(marker.lower() in line.lower() for marker in EXPLICIT_FORWARD_MARKERS)


#Normalizes
def normalize_header_line(line: str) -> str:
    """Return normalized header key (lowercase, no spaces before colon)."""
    line = line.lstrip()  # strip leading whitespace
    # Replace multiple spaces before colon with just one
    line = re.sub(r"\s*:\s*", ":", line, count=1)
    return line

#Checks for present header from EXPLICIT_FORWARD_MARKERS
def is_header_line(line: str) -> bool:
    line = normalize_header_line(line)
    return any(line.lower().startswith(h.lower()) for h in HEADER_KEYWORDS)

#Splits the text into new key-value pair if header is found.
def parse_header(line: str):
    """Return (key, value) tuple if line looks like a header."""
    line = normalize_header_line(line)
    if ":" in line:
        key, val = line.split(":", 1)
        key_norm =  key.strip().lower()
        canonical = HEADER_ALIASES.get(key_norm)
        if canonical:
            return canonical, val.strip()
    return None, None


#Loops through the lines and checks for headers/markers.
def find_forward_starts(lines, min_cluster_size=2):
    """Return sorted start indices of forwarded blocks.

    Triggers on:
      1) explicit markers,
      2) header clusters (>= min_cluster_size),
      3) fallback: lone 'From:' not at the very top (i > 3).
    """
    for i, l in enumerate(lines[:20]):  # first 20 lines
        print(repr(l))

    starts = []
    n = len(lines)
    i = 0
    while i < n:
        line = lines[i]

        # 1) explicit marker
        if is_forward_marker(line):
            starts.append(i)
            i += 1
            continue

        # 2) header cluster
        j = i
        cluster_size = 0
        while j < n and is_header_line(lines[j]):
            cluster_size += 1
            j += 1
        if cluster_size >= min_cluster_size:
            starts.append(i)
            i = j
            continue

        # 3) fallback: lone 'From:' deeper in the body
        if line.startswith(("From:", "Van:")) and i > 3:
            starts.append(i)
            i += 1
            continue

        i += 1

    # de-dup + sort
    starts = sorted(set(starts))
    return starts


def extract_forwarded_blocks(body_text: str, depth: int = 0):
    """Recursively extract forwarded blocks from body_text, segmenting by starts."""
    if depth >= MAX_DEPTH:
        return body_text.strip(), []
    
    #splits each 'body_text'
    lines = body_text.splitlines()
    print(lines)

    #finds indices where forward messages are
    starts = find_forward_starts(lines)
    print(starts)
    if not starts:
        return body_text.strip(), []

    # Build non-overlapping segments: [start_i, start_{i+1}) … last to end
    segments = []
    for idx, s in enumerate(starts):
        e = starts[idx + 1] if idx + 1 < len(starts) else len(lines)
        if s < e:
            segments.append((s, e))

    # Main body is everything before the first forwarded segment
    main_body = "\n".join(lines[:segments[0][0]]).strip()

    forwarded_blocks = []
    for (s, e) in segments:
        seg = lines[s:e]

        # Skip leading explicit forward marker line (like "Forwarded message")
        k = 0
        if seg and is_forward_marker(seg[0]):
            k = 1
            # also skip following blank lines
            while k < len(seg) and seg[k].strip() == "":
                k += 1

        # 🔑 NEW: make sure we *start* at the first "From:" / "Van:" line
        while k < len(seg) and not seg[k].lower().startswith(("from:", "van:")):
            k += 1

        # Now parse headers starting at the first header line
        headers = {}
        h = k
        while h < len(seg) and is_header_line(seg[h]):
            line = seg[h]
            norm_line = normalize_header_line(line).strip()

            lower_line = norm_line.lower()

            # 🔑 Explicit capture for key headers
            if lower_line.startswith(("from:", "van:")):
                headers["from"] = norm_line.split(":", 1)[1].strip()
            elif lower_line.startswith(("to:", "aan:")):
                headers["to"] = norm_line.split(":", 1)[1].strip()
            elif lower_line.startswith("cc:"):
                headers["cc"] = norm_line.split(":", 1)[1].strip()
            elif lower_line.startswith(("subject:", "onderwerp:")):
                headers["subject"] = norm_line.split(":", 1)[1].strip()
            elif lower_line.startswith(("date:", "datum:", "sent:", "verzonden:")):
                headers["date"] = norm_line.split(":", 1)[1].strip()
            else:
                # fallback to parse_header for anything else
                try:
                    key, val = parse_header(norm_line)
                    print("HEADER LINE:", repr(line), "->", key, "=", val)
                    if key:
                        headers[key] = val
                except ValueError:
                    pass  # malformed line; ignore

            h += 1

        # Skip one blank line after headers (common formatting)
        if h < len(seg) and seg[h].strip() == "":
            h += 1

        # Remaining content of this forwarded block
        content_lines = seg[h:]
        content_text = "\n".join(content_lines).strip()

        # Recurse on the content of THIS block only
        inner_body, inner_forwards = extract_forwarded_blocks(content_text, depth + 1)

        forwarded_blocks.append({
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "cc": headers.get("cc", ""),
            "subject": headers.get("subject", ""),
            "date": headers.get("date", ""),
            "body": inner_body,
            "forwarded": inner_forwards if inner_forwards else None
        })

    return main_body, forwarded_blocks




# -----------------------------
# Processing JSON files
# -----------------------------
def process_json(in_file, out_dir=OUTPUT_DIR):
    with open(in_file, "r", encoding="utf-8") as f:
        record = json.load(f)

    main_body, forwards = extract_forwarded_blocks(record.get("body", ""))

    record["body"] = main_body
    if forwards:
        record["forwarded"] = forwards

    # save
    out_file = out_dir / in_file.name
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    return out_file


def batch_process(in_dir=INPUT_DIR, out_dir=OUTPUT_DIR):
    json_files = list(in_dir.glob("*.json"))
    for jf in tqdm(json_files, desc="Unpacking forwards"):
        process_json(jf, out_dir)


if __name__ == "__main__":
    batch_process()