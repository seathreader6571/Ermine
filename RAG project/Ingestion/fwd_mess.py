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
DEBUG = True  # set to False to quiet the logs

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

# header line detector (robust to '>' quotes + spaces)
HEADER_START_RE = re.compile(
    r"^\s*>*\s*(from|van|to|aan|cc|bcc|subject|onderwerp|date|datum|sent|verzonden)\s*:\s*(.*)$",
    re.IGNORECASE,
)

def dprint(*args):
    if DEBUG:
        print(*args)

# ==============================
# HELPERS
# ==============================

def normalize(text: str) -> str:
    """Normalize newlines and strip odd characters & literal escapes."""
    if text is None:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n")
    text = text.replace("\uFEFF", "")
    return text

def show_context(txt: str, idx: int, span: int = 80) -> str:
    if idx < 0 or idx >= len(txt):
        return "(index out of range)"
    lo = max(0, idx - span)
    hi = min(len(txt), idx + span)
    return ("..." + txt[lo:hi] + "...").replace("\n", "\\n")

def find_forward_start(txt: str) -> int:
    """
    Keep it simple: prefer \\nfrom:/\\nvan: (case-insensitive).
    Also accept common forward markers if present.
    """
    s = "\n" + txt  # so search works at very start too

    # Optional: forward markers
    for mk in FORWARD_MARKERS:
        m = re.search(r"\n\s*" + re.escape(mk) + r"\b", s, flags=re.I)
        if m:
            pos = max(m.start() - 1, 0)
            dprint(f"[find_forward_start] Marker '{mk}' at index {pos}")
            return pos

    # Primary rule: \nfrom: / \nvan:
    m = re.search(r"\n(?:>+\s*)?(from|van)\s*:", s, flags=re.I)
    if m:
        pos = max(m.start() - 1, 0)
        dprint(f"[find_forward_start] Found '\\nfrom:/\\nvan:' at index {pos}")
        dprint("  Context:", show_context(txt, pos))
        return pos

    dprint("[find_forward_start] No header start found.")
    return -1

def parse_header_block(segment: str):
    """
    Minimal header parser:
      • read lines until blank line,
      • header lines = HEADER_START_RE,
      • continuation = any non-blank line that is NOT a header line
        (even without indentation).
    Returns (headers_dict or None, remainder_after_headers).
    """
    lines = segment.split("\n")
    headers = {k: None for k in ["from", "to", "cc", "bcc", "subject", "date"]}
    i, n = 0, len(lines)
    seen_any = False
    current_key = None

    dprint("[parse_header_block] Begin; lines:", n)

    while i < n:
        line_raw = lines[i]
        line = line_raw.rstrip("\r")
        dprint(f"  [L{i}] {repr(line)}")

        if not line.strip():
            dprint(f"  [L{i}] Blank → end headers")
            i += 1
            break

        m = HEADER_START_RE.match(line)
        if m:
            label = m.group(1).lower()
            value = (m.group(2) or "").strip()
            canon = LABEL_TO_CANON.get(label)
            if canon:
                headers[canon] = value if value else None
                current_key = canon
                seen_any = True
                dprint(f"    [HDR] {label!r} → {canon!r} = {value!r}")
                i += 1

                # CONTINUATIONS: consume any non-blank, non-header line(s)
                while i < n:
                    nxt = lines[i].rstrip("\r")
                    if not nxt.strip():
                        dprint(f"    [L{i}] Blank → stop cont")
                        i += 1
                        break
                    if HEADER_START_RE.match(nxt):
                        dprint(f"    [L{i}] Next header detected → stop cont")
                        break
                    # treat as continuation even if not indented
                    add = nxt.strip()
                    prev = headers[current_key] or ""
                    headers[current_key] = (prev + (" " if prev else "") + add).strip()
                    dprint(f"    [CONT] {current_key} += {add!r} → {headers[current_key]!r}")
                    i += 1
                continue
            else:
                dprint(f"    [WARN] Label {label!r} not in map → stop headers")
                break

        # If first line isn't a header, this segment isn't a header block
        if not seen_any:
            dprint(f"  [parse_header_block] First line not header → not a header block.")
            return None, segment
        else:
            dprint(f"  [parse_header_block] Non-header after some headers → end headers.")
            break

    remainder = "\n".join(lines[i:]).lstrip("\n")
    if not seen_any:
        dprint("[parse_header_block] No headers found in this segment.")
        return None, segment
    dprint("[parse_header_block] Parsed headers:", headers)
    return headers, remainder

def parse_forward_chain(body: str, depth: int = 0):
    if depth > MAX_DEPTH:
        dprint(f"[parse_forward_chain] Max depth {MAX_DEPTH} reached.")
        return None, []

    body = normalize(body)
    if not body.strip():
        dprint(f"[parse_forward_chain] Empty body at depth {depth}.")
        msg = {"depth": depth, "body": ""}
        return msg, [msg]

    dprint(f"[parse_forward_chain] Depth {depth}, body len {len(body)}")
    idx = find_forward_start(body)
    if idx == -1:
        msg = {"depth": depth, "body": body.strip()}
        dprint(f"[parse_forward_chain] No headers → leaf, body len {len(msg['body'])}")
        return msg, [msg]

    main_text = body[:idx].strip()
    fwd_segment = body[idx:].lstrip()
    dprint(f"[parse_forward_chain] Split at {idx}; main_text len={len(main_text)}")
    dprint("  fwd_segment head:", repr(fwd_segment[:80]))

    headers, rest = parse_header_block(fwd_segment)
    if headers is None:
        dprint("[parse_forward_chain] Header parse failed → treat as plain body")
        msg = {"depth": depth, "body": body.strip()}
        return msg, [msg]

    msg = {"depth": depth, "body": main_text}
    msg.update(headers)
    dprint(f"[parse_forward_chain] Captured headers at depth {depth}: {headers}")

    fwd_msg, flat = parse_forward_chain(rest, depth + 1)
    msg["fwd"] = fwd_msg
    return msg, [msg] + flat

# ==============================
# IO + BATCH
# ==============================

def process_file(path: Path):
    print(f"\n=== Processing {path.name} ===")
    try:
        data = json.load(path.open("r", encoding="utf-8"))
    except Exception as e:
        print(f"⚠️  Invalid JSON for {path.name}: {e}")
        return

    body = data.get("body", "")
    if not isinstance(body, str):
        print(f"⚠️  Skipped {path.name}: 'body' is not a string.")
        return

    dprint(f"[process_file] Raw body length: {len(body)}")
    nested, flat = parse_forward_chain(body)

    structured = {
        "source_file": path.name,
        "nested_structure": nested,
        "flattened_messages": flat,
    }

    out_path = OUTPUT_DIR / path.name
    try:
        json.dump(structured, out_path.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"✅  Saved → {out_path}")
    except Exception as e:
        print(f"❌  Failed to write {out_path.name}: {e}")

def batch_process():
    files = sorted(INPUT_DIR.glob("*.json"))
    if not files:
        print("No .json files found in input folder:", INPUT_DIR)
        return
    print(f"Found {len(files)} files in {INPUT_DIR}")
    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}]")
        process_file(f)

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    batch_process()