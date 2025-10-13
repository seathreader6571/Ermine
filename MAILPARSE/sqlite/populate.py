#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import json
from pathlib import Path
import sqlite3

conn = sqlite3.connect('emails.db') 
# dit is een veiligheidsrisico: we moeten via het script met de database verbinden, maar dan staat er een path in het script online...
# is de github repo prive?
c = conn.cursor()

# format table
# First the main table containing email metadata and filepaths
# We will store body text in a second, virtual FTS5 table for full-text search capabilities
# bij to, from en cc ff kijken of ie alleen de namen pakt of de hele mailadressen of allebei; wat willen we?
c.executescript('''
CREATE TABLE IF NOT EXISTS email_headers ( 
    id INTEGER PRIMARY KEY,
    date TEXT,
    sender TEXT, 
    recipient TEXT,
    cc TEXT,
    subject TEXT,
    pdf_path TEXT,
    has_attachments INTEGER, -- 0/1
    attachments_json TEXT -- JSON array of filenames/paths
    );

CREATE VIRTUAL TABLE IF NOT EXISTS email_data USING fts5(
    subject,
    body, 
    content='emails', 
    content_rowid='id');
''')
##################################################################
##################################################################
##################################################################

# To make use of pipeline, uses the extracted messages stored in python vars to populate the database immediately
# before writing to json file

import mail_only

INPUT_DIR = Path(r"C:/Users/drumm/Documents/ERMINE (deprecated)/mail_20250910_211624_conversion")
output_dir = Path(r"C:/Users/drumm/Documents/ERMINE (deprecated)/mail_20250910_211624_conversion/output")
# test with one, later batch_convert

parts = mail_only.parse_eml_clean(INPUT_DIR / "__XA1zThcubhjgyjqZ8eOOtzKAqVH5343WwhRbLf5MGV_b8yKvmE-gopce5DmMJJIzxsyEy_wKJy7TLscQ557g==.eml")
#         "from": from_,
#         "to": to_,
#         "cc": cc_,
#         "subject": subject,
#         "date": date,
#         "body": body,
# path gets created in process_eml while writing to json file, we want it separately

def construct_path(record): # RISK: constructs filenames parallel to mail_only.py. If different inputs, different outputs and database paths will be wrong
    """constructs a unique file path just like in mail_only.py, but now for the database record

    Args:
        record (_type_): _description_
    """
    # --- build filename from date + subject ---
    date_str = record["date"][:10] if record["date"] else "unknown_date"
    subject_str = mail_only.safe_filename(record["subject"])[:50]  # truncate long subjects
    base_name = f"{date_str}_{subject_str}"

    # --- ensure uniqueness ---
    out_file = output_dir / f"{base_name}.json"
    counter = 1
    while out_file.exists():
        out_file = output_dir / f"{base_name}_{counter}.json"
        counter += 1
    return str(out_file)

path = construct_path(parts)

# now populate the database
c.execute('''
INSERT INTO email_headers (date, sender, recipient, cc, subject, pdf_path, has_attachments, attachments_json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
''', (
    parts["date"],
    parts["to"],
    parts["from"],
    parts["cc"],
    parts["subject"],
    path,
    0, # has_attachments
    '[]' # attachments_json
))


c.execute('''
INSERT INTO email_data (rowid, subject)
    SELECT id, subject FROM email_headers;
''')\
# Copilot suggested after email_headers 'WHERE id = last_insert_rowid()'

c.execute('''
INSERT INTO email_data (rowid, body)
VALUES (last_insert_rowid(), ?)
''', (parts["body"],))

conn.commit()
conn.close()