#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sqlite3
import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(layout="wide")



BASE_DIR = os.path.dirname(__file__)
db_path = os.path.join(BASE_DIR, "emails.db")
logo_path = os.path.join(BASE_DIR, "resources/logo.png")

conn = sqlite3.connect(db_path)

st.title("Email Query Tool")


query = st.text_input("Enter your search query:")
subject = st.text_input("Filter by subject:")
sender = st.text_input("Filter by sender (from):")
recipient = st.text_input("Filter by recipient (to):")
date_from = st.date_input("From date:")
date_to = st.date_input("To date:")
limit = st.number_input("Number of emails to load (affects speed):", min_value=50, max_value=2000, value=200, step=50)


# Handle empty queries
if not query:
    sql = """
    SELECT 
        eh.id, 
        eh.date, 
        eh.sender, 
        eh.recipient, 
        eh.cc, 
        eh.subject, 
        eh.body,
        eh.html,
        eh.eml_path
    FROM email_headers eh
    """ 
else: 
    sql = """
    SELECT 
        eh.id, 
        eh.date, 
        eh.sender, 
        eh.recipient, 
        eh.cc, 
        eh.subject, 
        eh.body,
        eh.html,
        eh.eml_path
    FROM email_headers eh
    JOIN email_data ed ON eh.id = ed.rowid
    """

params = []

# Build WHERE clauses
where_clauses = []
if query.strip():
    where_clauses.append("ed.body MATCH ?")
    params.append(query.strip())

if subject.strip():
    where_clauses.append('eh.subject LIKE ?')
    params.append(f"%{subject.strip()}%")

if sender.strip():
    where_clauses.append('eh.sender LIKE ?')
    params.append(f"%{sender.strip()}%")

if recipient.strip():
    where_clauses.append('eh.recipient LIKE ?')
    params.append(f"%{recipient.strip()}%")

if date_from is not None and date_to is not None and date_from < date_to:
    where_clauses.append('DATE(eh.date) BETWEEN DATE(?) AND DATE(?)')
    params.append(date_from.isoformat())
    params.append(date_to.isoformat())    

if where_clauses:
    sql += " WHERE " + " AND ".join(where_clauses)

sql += f" LIMIT {limit}"

df = pd.read_sql_query(sql, conn, params=params)

st.dataframe(df)


for idx, row in df.iterrows():
    cols = st.columns([3, 1, 1])  # main content / expand button
    cols[0].write(f"**{row['subject']}** | Sent by {row['sender']} on {row['date']}")

    if row['eml_path']:
        eml_path = Path(row['eml_path'])
        if eml_path.exists():
            with open(eml_path, "rb") as f:
                eml_bytes = f.read()
            cols[2].download_button(
                label="✉️ Open EML",
                data=eml_bytes,
                file_name=eml_path.name,
                mime="message/rfc822"
            )


    if cols[1].button("View", key=f"view_{idx}"):
        st.markdown(f"<div style='margin-left:20px'>{row['html']}</div>", unsafe_allow_html=True)
     # Separator between rows
    st.markdown("---")  # horizontal line