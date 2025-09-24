#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import re
import html
import shutil
from tqdm import tqdm
import logging
from datetime import datetime
from pathlib import Path

INPUT_PATH  = Path(r"D:\mywritingpad@proton.me\mail_20250910_211624")
OUTPUT_PATH = Path(r"D:\mywritingpad@proton.me\output_txt")

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime


# -------------------
# Logging setup
# -------------------
logging.basicConfig(
    filename='eml_mailparser_attachments.log',
    filemode="a",          # append instead of overwrite
    encoding="utf-8",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    )



# -------------------
# Utilities
# -------------------
def safe_filename(text):
    if not text:  # catches None, empty string, etc.
        return "unknown"
    # Replace spaces with underscores
    text = text.replace(" ", "_")
    # Remove unsafe characters for filenames, including []
    return re.sub(r'[<>:"/\\|?*\-\[\]]', '', text)

def unique_filename(base_path: Path) -> Path:
    """
    Ensure filename is unique by appending a short suffix (1a, 1b, ...).
    """
    if not base_path.exists():
        return base_path

    counter = 1
    while True:
        new_path = base_path.with_name(f"{base_path.stem}_{counter}{base_path.suffix}")
        if not new_path.exists():
            logging.info(f"⚠️ Filename conflict for {base_path.name}, using {new_path.name} instead.")
            return new_path
        counter += 1

# -------------------
# Parse .eml content
# -------------------
def parse_eml(eml_file):
    with open(eml_file, "rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)

    headers = {
        "Subject": msg["subject"],
        "From": msg["from"],
        "To": msg["to"],
        "Cc": msg["cc"],
        "Date": msg["date"],
    }

    body = ""
    attachments = []

    for part in msg.walk():
        ctype = part.get_content_type()
        disp = (part.get("Content-Disposition") or "").lower()

        # Body
        if ctype in ("text/plain", "text/html") and "attachment" not in disp:
            body = part.get_content()

        # Regular attachments (no Content-ID, has filename)
        elif part.get_filename():
            attachments.append((part.get_filename(), part.get_payload(decode=True), ctype))
    
    if body is None:
        body = ""

    return headers, body, attachments

def save_email_and_attachments(headers, body, attachments, base_name, out_root):
    emails_dir = out_root / "emails"
    attachments_dir = out_root / "attachments"
    emails_dir.mkdir(parents=True, exist_ok=True)
    attachments_dir.mkdir(parents=True, exist_ok=True)

    # Save email body
    email_txt = emails_dir / f"{base_name}.txt"
    with open(email_txt, "w", encoding="utf-8") as f:
        f.write(f"Subject: {headers.get('Subject') or ''}\n")
        f.write(f"From: {headers.get('From') or ''}\n")
        f.write(f"To: {headers.get('To') or ''}\n")
        f.write(f"Cc: {headers.get('Cc') or ''}\n")
        f.write(f"Date: {headers.get('Date') or ''}\n")
        f.write("\n--- Body ---\n")
        f.write(body if body else "[No body]")

    # Save attachments
    for idx, (fname, data, ctype) in enumerate(attachments, start=1):
        safe_name = safe_filename(fname) or f"attachment_{idx}"
        att_txt = attachments_dir / f"{base_name}_{safe_name}.txt"

        try:
            text_data = data.decode("utf-8", errors="replace")
        except Exception:
            text_data = f"[Non-text attachment: {fname}, type={ctype}, {len(data)} bytes]"

        with open(att_txt, "w", encoding="utf-8") as f:
            f.write(text_data)

    return email_txt



# -------------------
# Process one file
# -------------------
def process_eml(eml_file, output_root=None, skip_if_exists=True):
    eml_file = Path(eml_file)

    # Determine output root
    if output_root is None:
        output_root = eml_file.parent / "output_txt"
    else:
        output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    # Parse
    headers, body, attachments = parse_eml(eml_file)

    # Construct base filename
    if headers["Date"]:
        try:
            dt = parsedate_to_datetime(headers["Date"])
            date_str = dt.strftime("%Y%m%d")
        except Exception:
            date_str = "unknown_date"
    else:
        date_str = "unknown_date"

    subject_str = safe_filename(headers["Subject"]) or "no_subject"
    base_name = f"{date_str}_{subject_str}"

    # Skip logic
    email_file = output_root / "emails" / f"{base_name}.txt"
    if skip_if_exists and email_file.exists():
        return None

    return save_email_and_attachments(headers, body, attachments, base_name, output_root)


# -------------------
# Batch runner
# -------------------
def batch_convert(eml_files, out_dir=None, workers=8, skip_if_exists=True):
    # Add a header for this run
    logging.info("=" * 60)
    logging.info(f"Starting new conversion run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 60)


    results = []
    skipped = []
    to_process = []
    failed = []

    for f in eml_files:
        f = Path(f)
        if out_dir is None:
            target_dir = f.parent / "output"
        else:
            target_dir = Path(out_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        # 🔹 Parse headers to build consistent filename
        headers, _, _ = parse_eml(f)

        if headers["Date"]:
            try:
                dt = parsedate_to_datetime(headers["Date"])
                date_str = dt.strftime("%Y%m%d")
            except Exception:
                date_str = "unknown_date"
        else:
            date_str = "unknown_date"

        subject_str = safe_filename(headers["Subject"]) or "no_subject"
        base_name = f"{date_str}_{subject_str}"

        # 🔹 Look for existing PDF(s) with this base_name
        existing = list(target_dir.glob(f"{base_name}*.txt"))
        if skip_if_exists and existing:
            skipped.append(f)
        else:
            to_process.append(f)
    logging.info(f"Skipping {len(skipped)} files that already have output TXTs.")
    logging.info(f"Processing {len(to_process)} files.")


    # print how many duplicate files found and skipped
    if len(skipped) > 0:
        print(f"Skipping {len(skipped)} files that already have output TXTs.")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_file = {executor.submit(process_eml, f, out_dir): f for f in to_process}
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing emails"):
            eml_file = future_to_file[future]
            try:
                pdf_path = future.result()
                if pdf_path:
                    logging.info(f"✔ Converted: {eml_file} → {pdf_path}")
                    results.append(pdf_path)
                else:
                    logging.warning(f"ℹ No TXT generated for {eml_file} (skipped)")
                    skipped.append(eml_file)
            except Exception as e:
                logging.error(f"⚠ Error processing {eml_file}: {e}")
                failed.append((eml_file, str(e)))

    # --- Summary log ---
    print("\nBatch conversion summary:")
    print(f"  ✅ Processed: {len(results)}")
    print(f"  ⏭️ Skipped:   {len(skipped)}")
    print(f"  ❌ Failed:    {len(failed)}")
    print(f"  See 'eml_mailparser_attachments.log' for details.")
    logging.info("=" * 60)
    logging.info(f"Batch conversion summary: Processed={len(results)}, Skipped={len(skipped)}, Failed={len(failed)}")
    logging.info("=" * 60)

    if failed:
        print("\nFailed files:")
        for f, err in failed[:10]:  # show only first 10 to avoid spam
            print(f"   {f} -> {err}")
        if len(failed) > 10:
            print(f"   ... and {len(failed)-10} more")
    return results



# -------------------
# Command-line interface
# -------------------
if __name__ == "__main__":
    target = INPUT_PATH
    out_dir = OUTPUT_PATH

    if not target.exists():
        print(f"Error: Path not found: {target}")
        sys.exit(1)

    if target.is_file() and target.suffix.lower() == ".eml":
        # Single .eml file
        txt_path = process_eml(target, out_dir)
        print(f"✔ Converted single file to: {txt_path}")

    elif target.is_dir():
        # Folder mode: process all .eml files
        eml_files = list(target.glob("*.eml"))
        if not eml_files:
            print(f"No .eml files found in {target}")
            sys.exit(1)

        print(f"Found {len(eml_files)} .eml files in {target}, processing...")
        batch_convert(eml_files, out_dir, workers=4)

    else:
        print("Error: Must provide a .eml file or a folder containing .eml files")
