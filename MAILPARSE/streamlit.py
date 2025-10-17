#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import sqlite3
import streamlit as st
import pandas as pd

conn = sqlite3.connect('emails.db')

st.title("Email Query Tool")

query = st.text_input("Enter your search query:")
sender = st.text_input("Filter by sender (from):")
date_from = st.date_input("From date:")
date_to = st.date_input("To date:")


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
        eh.pdf_path, 
        eh.has_attachments, 
        eh.attachments_json,
        eh.body
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
        eh.pdf_path, 
        eh.has_attachments, 
        eh.attachments_json, 
        ed.body
    FROM email_headers eh
    JOIN email_data ed ON eh.id = ed.rowid
    """

params = []

# Build WHERE clauses
where_clauses = []
if query.strip():
    where_clauses.append("ed.body MATCH ?")
    params.append(query.strip())

if sender.strip():
    where_clauses.append('eh.sender LIKE ?')
    params.append(f"%{sender.strip()}%")

if date_from is not None and date_to is not None and date_from < date_to:
    where_clauses.append('DATE(eh.date) BETWEEN DATE(?) AND DATE(?)')
    params.append(date_from.isoformat())
    params.append(date_to.isoformat())    

if where_clauses:
    sql += " WHERE " + " AND ".join(where_clauses)

df = pd.read_sql_query(sql, conn, params=params)

st.dataframe(df, width=2000)