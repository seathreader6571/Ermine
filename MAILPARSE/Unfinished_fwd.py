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
INPUT_DIR_seathreader = Path(r"A:\Ermine\mywritingpad@proton.me\Testing\Input")
OUTPUT_DIR_seathreader = Path(r"A:\Ermine\mywritingpad@proton.me\Testing\Output")

INPUT_DIR_drummingsnipe = Path(r"C:/Users/drumm/Documents/ERMINE (deprecated)/testbatch/output_json")
OUTPUT_DIR_drummingsnipe = Path(r"C:/Users/drumm/Documents/ERMINE (deprecated)/testbatch/splitted_json")

OUTPUT_DIR_drummingsnipe.mkdir(parents=True, exist_ok=True)
MAX_DEPTH = 15
DEBUG = True  # set to False to quiet the logs

FORWARD_MARKERS = (
    "Begin forwarded message",
    "Forwarded message",
    "Original Message",
    "op"
)

# localized label → canonical key
LABEL_TO_CANON = {
    "from:": "from:", "van:": "from:",
    "to:": "to:", "aan:": "to:",
    "cc:": "cc:",
    "bcc:": "bcc:",
    "subject:": "subject:", "onderwerp:": "subject:",
    "date:": "date:", "datum:": "date:", "sent:": "date:", "verzonden:": "date:",
}

HEADER_KEYWORDS = {
    'Dutch': ('aan:', 'onderwerp:', 'datum:', 'verzonden:', 'cc:'),
    'English': ('to:', 'subject:', 'date:', 'sent:', 'cc:')
}
HEADER_RE = re.compile(r"(?im)^(from|van|to|aan|cc|bcc|subject|onderwerp|date|datum|sent|verzonden)\s*:\s*(.*)$")
#-------------------------------------
# HELPER-FUNCTIONS
#-------------------------------------

def normalize(text: str) -> str:
    """Normalize newlines and strip odd characters & literal escapes."""
    if text is None:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n")
    text = text.replace("/n", "\n")
    text = text.replace("\uFEFF", "")
    return text

def change_key_format(msg_dict: dict) -> dict:
    new_dict = {f"{key}:": v for key, v in msg_dict.items()} 
    return(new_dict)

def keyword_to_language(kw: str)-> str:
    if kw == 'van:':
        return('Dutch')
    if kw == 'from:':
        return('English')
    else:
        print('No proper header start, something wrong')


def list_merge(ls: list) -> str:
    delim = "\n"
    return(delim.join(ele for ele in ls))


#THe function below takes the dictionary and the 'body' text and splits it recursively to a new dictionary for every header start occurence.

def recursive_split(msg_dict: dict, depth = 0, max_depth = MAX_DEPTH) -> dict:
    body = normalize(msg_dict['body:']).strip()
    splitted = body.splitlines()

    fwd_start = next(
        (i for i, ln in enumerate(splitted)
            if ln.casefold().startswith(('from:', 'van:'))),
        None
    )
    if fwd_start is None:
        return msg_dict  # <-- important base case
    
    #Distribute the split bodies
    old_body = list_merge(splitted[: fwd_start])
    msg_dict['body:'] = old_body

    new_body = list_merge(splitted[fwd_start + 1:])
    new_dict = {  
        "from:": "Name",
        "to:": "Name",
        "cc:": "Name",
        "subject:": "some text",
        "date:": "some date",
        "body:": new_body


    }
    # Recurse into the "previous" message
    msg_dict['fwd_mess'] = recursive_split(new_dict, depth+1, max_depth)
    return msg_dict



#The function below takes the nested dictionaries and redistributes the proper headers.

def redistribute_json(dc: dict) -> dict:
    """
    Recursively splits and maps sections of an email-like dict using header keywords.
    Example header keys: "From:", "To:", "Subject:", etc.
    """
    body = normalize(dc['body:']).strip()

    def find_first(body: str) -> int | None:
        """Return index of first header line based on LABEL_TO_CANON keys."""
        splitted = body.casefold().splitlines()
        for i, line in enumerate(splitted):
            if any(line.startswith(prefix) for prefix in LABEL_TO_CANON.keys()):
                print(f"found Header key {body.splitlines()[i]}")
                return i
        print("No Headers found")
        return None

    # --- Initial split to extract 'from' section before first header ---
    splitted = body.splitlines()
    first_idx = find_first(body)
    if first_idx is not None and first_idx > 0:
        dc['from:'] = "\n".join(splitted[:first_idx])
        dc['body:'] = "\n".join(splitted[first_idx:])
    else:
        dc['body:'] = body

    def recursive_body_split(body_text: str, container: dict):
        """Recursively find headers and map their content."""
        lines = body_text.splitlines()
        if not lines:
            return

        # Base case: no recognizable header found
        header_line = lines[0].casefold().strip()
        if not any(header_line.startswith(k) for k in LABEL_TO_CANON.keys()):
            print(f"There was no header found in {header_line}")
            container['body:'] = "\n".join(lines)
            return

        # Identify header key and canonical name
        header_key = next(
            (k for k in LABEL_TO_CANON.keys() if header_line.startswith(k)),
            None
        )
        canon_key = LABEL_TO_CANON[header_key]

        # Find next header index
        remaining_text = "\n".join(lines[1:])
        next_idx = find_first(remaining_text)

        if next_idx is not None:
            # Split current section and recurse into remainder
            current_section = "\n".join(remaining_text.splitlines()[:next_idx])
            remainder = "\n".join(remaining_text.splitlines()[next_idx:])
            container["body:"] = remainder
            container[canon_key] = current_section.strip()
            recursive_body_split(remainder, container)
        else:
            # Last section — assign all remaining text
            split = remaining_text.splitlines()
            container[canon_key] = split[0]
            container["body:"] = "\n".join(split[1:])

    # --- Perform recursive header mapping ---
    recursive_body_split(dc['body:'], dc)

    # --- If nested forwarded message exists, process recursively ---
    if 'fwd_mess' in dc:
        print("found forward message")
        dc['fwd_mess'] = redistribute_json(dc['fwd_mess'])

    return dc




    

        

    




#----------------------------
#   Process Files in batch
#----------------------------

def process_json(path: Path):
    print(f"/n === Processing {path.name} =====  ")
    try:
        data = json.load(path.open("r", encoding="utf-8"))
    except Exception as e:
        print(f"⚠️  Invalid JSON for {path.name}: {e}")
        return
    if not isinstance(data, dict):
        print(f"file {path.name} is not a json-dict.")
        return
    updated = change_key_format(data)
    structured = recursive_split(updated)
    structured_distributed = redistribute_json(structured)
    out_path = OUTPUT_DIR_drummingsnipe / path.name
    try:
        json.dump(structured_distributed, out_path.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"✅  Saved → {out_path}")
    except Exception as e:
        print(f"❌  Failed to write {out_path.name}: {e}")

def batch_process():
    files = sorted(INPUT_DIR_drummingsnipe.glob("*.json"))
    if not files:
        print(f"no json files found in {INPUT_DIR_drummingsnipe.name}")
        return
    print(f"Found {len(files)} files in {INPUT_DIR_drummingsnipe}")
    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}]")
        process_json(f)

#----------------------------
#   MAIN
#----------------------------
if __name__ == "__main__":
    batch_process()