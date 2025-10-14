#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
from pathlib import Path
from httpx import get
from tqdm import tqdm
import logging
import sqlite3
import json
from datetime import datetime

# Path to JSON files
json_folder = Path(r'c:/Users/drumm/Documents/ERMINE (deprecated)/testbatch/output_json')


# Connect to SQLite database (or any other)
conn = sqlite3.connect('mailparse/emails.db')
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
    attachments_json TEXT, -- JSON array of filenames/paths
    body TEXT
    );

CREATE VIRTUAL TABLE IF NOT EXISTS email_data USING fts5(
    subject,
    body, 
    content='email_headers', 
    content_rowid='id');
''')

# Prepare for batch insertion
header_rows = []
fts_rows = []

# Loop over all JSON files
for file_path in json_folder.glob("*.json"):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        # Prepare data for email_headers
        
    header_rows.append((
        data.get("date"),
        data.get("from"),
        data.get("to"),
        data.get("cc"),
        data.get("subject"),
        str(file_path),
        0,          # has_attachments
        '[]',        # attachments_json
        data.get("body")
    ))


# Use transaction for speed
with conn:
    for file_path in json_folder.glob("*.json"):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Insert headers
        c.execute('''
            INSERT INTO email_headers (date, sender, recipient, cc, subject, pdf_path, has_attachments, attachments_json, body)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get("date"),
                data.get("from"),
                data.get("to"),
                data.get("cc"),
                data.get("subject"),
                str(file_path),
                0,          # has_attachments
                '[]',        # attachments_json
                data.get("body")
            ))      

        # Insert full-text data
        c.execute('''
            INSERT INTO email_data (rowid, subject, body)
            VALUES (last_insert_rowid(), ?, ?)
        ''', (
            data.get("subject"),
            data.get("body")
        ))


# conn.commit()
# conn.close()