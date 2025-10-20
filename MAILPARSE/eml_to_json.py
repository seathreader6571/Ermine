#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
import sys



#----------------------------------------
# This script: .eml --> .json
#----------------------------------------


INPUT_DIR_seathreader = Path(r"J:\Ermine\mywritingpad@proton.me\mail_20250910_211624")
OUTPUT_DIR_seathreader = Path(r"J:\Ermine\mywritingpad@proton.me\output_txt\emails")

INPUT_DIR_drummingsnipe = Path(r"C:/Users/drumm/Documents/ERMINE_local/mail_20250910_211624_conversion")
OUTPUT_DIR_drummingsnipe = Path(r"C:/Users/drumm/Documents/ERMINE_local/mail_20250910_211624_conversion/output_json")

# -------------------
# Logging setup
# -------------------
logging.basicConfig(
    filename='eml_parser_clean.log',
    filemode="a",
    encoding="utf-8",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -------------------
# Utilities
# -------------------
def safe_filename(text):
    """Remove unsafe characters from filenames."""
    text = text.strip().replace(" ", "_")
    return re.sub(r'[<>:"/\\|?*]', '', text) or "no_subject"

def render_html(body):
    # Basic HTML template
    template = f"""
    <html>
      <head>
        <meta charset="utf-8">
        <style>
          body {{ font-family: Arial, sans-serif; }}
          .headers {{ margin-bottom: 20px; font-size: 14px; color: #000000; }}
          .subject {{ font-weight: bold; font-size: 16px; color: #000000; }}
          hr {{ margin: 15px 0; }}

          /* Override opacity and color in forwarded threads */
          .body, .body * {{
          color: black !important;
          opacity: 1 !important;
      }}
        </style>
      </head>
      <body>
        <hr/>
        <div class="body">{body}</div>
      </body>
    </html>
    """
    return template

# -------------------
# Parser
# -------------------
def parse_eml_clean(eml_file):
    with open(eml_file, "rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)

    # --- Extract headers ---
    from_ = msg.get("from", "")
    to_ = msg.get("to", "")
    cc_ = msg.get("cc", "")
    subject = msg.get("subject", "")
    date = msg.get("date", "")

    # Normalize date
    try:
        date = parsedate_to_datetime(date).isoformat()
    except Exception:
        pass

    # --- Extract body ---
    body = None
    for part in msg.walk():
        ctype = part.get_content_type()
        disp = (part.get("Content-Disposition") or "").lower()

        if "attachment" in disp:
            continue  # skip attachments

        if ctype == "text/plain" and body is None:
            body = part.get_content().strip()
        elif ctype == "text/html" and body is None:
            raw_html = part.get_content()
            soup = BeautifulSoup(raw_html, "html.parser")
            body = soup.get_text(separator="\n", strip=False)
            

    if not body:
        body = ""

    # Normalize whitespace
    body = re.sub(r"\n\s*\n+", "\n\n", body.strip())

    html_body = render_html(body)

    return {
        "from": from_,
        "to": to_,
        "cc": cc_,
        "subject": subject,
        "date": date,
        "body": body,
        "html": html_body,
        "eml_path": str(eml_file)
    }

# -------------------
# Save single JSON file
# -------------------
def process_eml(eml_file, output_dir):
    eml_file = Path(eml_file)
    record = parse_eml_clean(eml_file)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- build filename from date + subject ---
    date_str = record["date"][:10] if record["date"] else "unknown_date"
    subject_str = safe_filename(record["subject"])[:50]  # truncate long subjects
    base_name = f"{date_str}_{subject_str}"

    # --- ensure uniqueness ---
    out_file = output_dir / f"{base_name}.json"
    counter = 1
    while out_file.exists():
        out_file = output_dir / f"{base_name}_{counter}.json"
        counter += 1

    # --- write JSON ---
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    logging.info(f"✔ Saved {eml_file} → {out_file}")
    return out_file

# -------------------
# Batch runner
# -------------------
def batch_convert(eml_files, out_dir, workers=4):
    results, failed = [], []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_file = {
            executor.submit(process_eml, f, out_dir): f for f in eml_files
        }
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing emails"):
            eml_file = future_to_file[future]
            try:
                out_file = future.result()
                results.append(out_file)
            except Exception as e:
                logging.error(f"⚠ Error processing {eml_file}: {e}", exc_info=True)
                failed.append((eml_file, str(e)))

    print(f"\n✅ Processed: {len(results)}")
    print(f"❌ Failed:    {len(failed)}")
    return results, failed

# -------------------
# CLI entry
# -------------------
if __name__ == "__main__":

    input_dir = INPUT_DIR_drummingsnipe
    output_dir = OUTPUT_DIR_drummingsnipe

    if not input_dir.exists():
        print(f"Error: Input folder not found: {input_dir}")
        sys.exit(1)

    eml_files = list(input_dir.glob("*.eml"))
    if not eml_files:
        print(f"No .eml files found in {input_dir}")
        sys.exit(1)

    print(f"Found {len(eml_files)} .eml files, processing...")
    batch_convert(eml_files, output_dir, workers=4)
