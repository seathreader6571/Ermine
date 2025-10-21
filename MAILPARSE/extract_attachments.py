#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import sys
import os
import re
import logging
from pathlib import Path
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor
# from datetime import datetime
from tqdm import tqdm


#----------------------------------------
# This script: .eml --> .json
#----------------------------------------

in_dir = Path(r"C:/Users/drumm/Documents/ERMINE_local/mail_20250910_211624_conversion")
out_dir = Path(r"C:/Users/drumm/Documents/ERMINE_local/attachments")
os.makedirs(out_dir, exist_ok=True)

# -------------------
# Logging setup
# -------------------
logging.basicConfig(
    filename='extract_attachments.log',
    filemode="a",
    encoding="utf-8",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Allowed attachment extensions (lowercase, with leading dot)
ALLOWED_EXTENSIONS = {
    ".pdf", ".docx", ".doc", ".xlsx", ".xls", ".rtf",
    ".htm", ".html", ".pptx", ".png", ".jpg", ".jpeg", ".zip",
    ".csv", ".mp4"
}

# -------------------
# Utilities
# -------------------
def sanitize_filename(text):
    """
    Creates a safe filename by:
    1. Handling None/empty values
    2. Converting spaces to underscores
    3. Keeping only alphanumeric chars, dots, and underscores
    """
    if not text:
        return "unknown"
    
    # Replace spaces with underscores
    text = text.replace(" ", "_")
    # Keep only safe characters
    return re.sub(r'[^A-Za-z0-9_.-]', '_', text)



def export_attachments(file_path):
    """Extract allowed attachments from a single .eml file."""
    exported_files = []
    try:
        with open(file_path, "rb") as f:
            msg = BytesParser(policy=policy.default).parse(f)

            # Extract email date
            date = msg.get('Date')
            # Normalize date
            try:
                date = parsedate_to_datetime(date)
                date_str = date.strftime('%Y%m%d')
                logging.info(f"Parsed date: {date_str}")
            except Exception as e:
                date_str = "unknown_date"
                logging.error(f"⚠️ Failed to parse date '{date}' in {file_path}: {e}")

            # Extract subject
            subject = msg.get('Subject', 'no_subject')
            subject_sanitized = sanitize_filename(subject[:50])

            # Loop through attachments
            for part in msg.iter_attachments():
                attach_filename = part.get_filename()
                if not attach_filename:
                    continue

                ext = os.path.splitext(attach_filename)[1].lower()
                if ext not in ALLOWED_EXTENSIONS:
                    logging.info(f"⏩ Skipped (unwanted type): {attach_filename}")
                    continue

                attach_sanitized = sanitize_filename(attach_filename)
                export_name = f"{date_str}_{subject_sanitized}_{attach_sanitized}"
                export_path = out_dir / export_name

                try:
                    with open(export_path, "wb") as out_file:
                        out_file.write(part.get_payload(decode=True))
                    logging.info(f"📥 Exported: {export_path}")
                    exported_files.append(str(export_path))
                except Exception as e:
                    logging.error(f"⚠️ Failed to export {attach_filename} from {file_path}: {e}")

    except Exception as e:
        logging.error(f"⚠️ Failed to parse {file_path}: {e}")

    return exported_files

# -------------------
# Gather all .eml files
# -------------------
eml_files = [entry.path for entry in os.scandir(in_dir) if entry.is_file() and entry.name.lower().endswith(".eml")]

# -------------------
# Parallel extraction (ThreadPoolExecutor for terminal)
# -------------------
all_exported_files = []
with ThreadPoolExecutor() as executor:
    for exported_files in tqdm(executor.map(export_attachments, eml_files), total=len(eml_files), desc="Extracting attachments"):
        all_exported_files.extend(exported_files)

print(f"✅ Total .eml files processed: {len(eml_files)}")
print(f"📎 Total attachments exported: {len(all_exported_files)}")
