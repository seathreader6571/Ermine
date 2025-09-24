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


import mailparser
import pdfkit
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from PyPDF2 import PdfMerger
import tempfile

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
    inline_images = {}

    for part in msg.walk():
        ctype = part.get_content_type()
        disp = (part.get("Content-Disposition") or "").lower()
        content_id = part.get("Content-ID")

        # Body
        if ctype == "text/html" and "attachment" not in disp:
            body = part.get_content()

        # Inline images (has Content-ID)
        elif content_id is not None:
            cid = content_id.strip("<>")
            fname = part.get_filename() or f"{cid}.img"
            inline_images[cid] = (fname, part.get_payload(decode=True), ctype)

        # Regular attachments (no Content-ID, has filename)
        elif part.get_filename():
            attachments.append((part.get_filename(), part.get_payload(decode=True), ctype))
    
    # Ensure body is always a string
    if body is None:
        body = ""


    return headers, body, attachments, inline_images


# -------------------
# Render HTML with inline images
# -------------------
def render_html(headers, body, inline_images, temp_dir):
    temp_dir.mkdir(parents=True, exist_ok=True)

    for cid, (fname, data, ctype) in inline_images.items():
        try:
            ext = ctype.split("/")[-1]
            img_path = temp_dir / f"{cid}.{ext}"
            with open(img_path, "wb") as f:
                f.write(data)
            body = body.replace(f"cid:{cid}", f"file:///{img_path.resolve().as_posix()}")
        except Exception as e:
            logging.error(f"⚠️ Failed to embed inline image {cid}: {e}")
            # Replace only the <img> referencing this CID with a small square X
            pattern = rf'<img[^>]+src=["\']cid:{re.escape(cid)}["\'][^>]*>'
            placeholder = (
                '<div style="display:inline-block;width:20px;height:20px;'
                'border:1px solid #999;text-align:center;line-height:20px;'
                'font-size:14px;color:#999;">✕</div>'
            )
            body = re.sub(pattern, placeholder, body, flags=re.IGNORECASE)

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
        </style>
      </head>
      <body>
        <div class="headers">
          <div class="subject">Subject: {html.escape(headers['Subject'])}</div>
          <div><strong>From:</strong> {html.escape(headers['From'])}</div>
          <div><strong>To:</strong> {html.escape(headers['To'])}</div>
          <div><strong>Cc:</strong> {html.escape(headers['Cc'])}</div>
          <div><strong>Date:</strong> {headers['Date']}</div>
        </div>
        <hr/>
        <div class="body">{body}</div>
      </body>
    </html>
    """
    return template


# -------------------
# Convert to PDF
# -------------------
def convert_to_pdf(html_string, pdf_path):
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    config = pdfkit.configuration(wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe")
    # Allow local file access (needed for inline images in emails)
    options = {
        "enable-local-file-access": None,
        'disable-external-links': '',
        'no-images': False,  # still allow local images
        'load-error-handling': 'ignore',
    }

    pdfkit.from_string(html_string, str(pdf_path), configuration=config, options=options)

# -------------------
# Merge PDFs
# -------------------
def merge_pdfs(pdf_files, output_pdf):
    merger = PdfMerger()
    for pdf in pdf_files:
        merger.append(pdf)
    merger.write(output_pdf)
    merger.close()

# -------------------
# Process one file
# -------------------
def process_eml(eml_file, output_root=None, cleanup_temp=True, skip_if_exists=True):
    eml_file = Path(eml_file)

    # Determine output root
    if output_root is None:
        output_root = eml_file.parent / "output"
    else:
        output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    # Parse
    headers, body, attachments, inline_images = parse_eml(eml_file)

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

    # Determine expected output files, give duplicate-safe names
    email_pdf = output_root / f"{base_name}.pdf"
    email_pdf = unique_filename(email_pdf)
    merged_pdf = output_root / f"{base_name}_attachm.pdf"
    merged_pdf = unique_filename(merged_pdf)

    # --- Skip logic ---
    if skip_if_exists and (merged_pdf.exists() or email_pdf.exists()):
        return None  # <--- Important: signals "skipped" to batch_convert

    # Create per-email temp folder for inline images
    temp_dir = output_root / f"temp_{eml_file.stem}"
    temp_dir.mkdir(exist_ok=True)

    # Render HTML with inline images
    html = render_html(headers, body, inline_images, temp_dir)

    # Convert to PDF
    convert_to_pdf(html, email_pdf)

    # Save attachments
    attachments_dir = output_root / "attachments"
    attachments_dir.mkdir(exist_ok=True)
    pdfs_to_merge = [email_pdf]

    for fname, data, ctype in attachments:
        att_path = attachments_dir / safe_filename(fname)
        with open(att_path, "wb") as f:
            f.write(data)
        if ctype == "application/pdf":
            pdfs_to_merge.append(att_path)

    # Merge PDFs if needed
    if len(pdfs_to_merge) > 1:
        merge_pdfs(pdfs_to_merge, merged_pdf)
        # remove the naked email PDF to save space
        email_pdf.unlink(missing_ok=True)
        final_pdf = merged_pdf
    else:
        final_pdf = email_pdf

    # Optional cleanup of temp inline images
    if cleanup_temp and temp_dir.exists():
        shutil.rmtree(temp_dir)

    return final_pdf


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
        headers, _, _, _ = parse_eml(f)

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
        existing = list(target_dir.glob(f"{base_name}*.pdf"))
        if skip_if_exists and existing:
            skipped.append(f)
        else:
            to_process.append(f)
    logging.info(f"Skipping {len(skipped)} files that already have output PDFs.")
    logging.info(f"Processing {len(to_process)} files.")


    # print how many duplicate files found and skipped
    if len(skipped) > 0:
        print(f"Skipping {len(skipped)} files that already have output PDFs.")

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
                    logging.warning(f"ℹ No PDF generated for {eml_file} (skipped)")
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
    if len(sys.argv) < 2:
        print("Usage: python eml_mailparser.py <file.eml or directory> [output_directory (optional)]")
        sys.exit(1)

    target = Path(sys.argv[1])
    out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else None

    if not target.exists():
        print(f"Error: Path not found: {target}")
        sys.exit(1)

    if target.is_file() and target.suffix.lower() == ".eml":
        # Single .eml file
        pdf_path = process_eml(target, out_dir)
        print(f"✔ Converted single file to: {pdf_path}")

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