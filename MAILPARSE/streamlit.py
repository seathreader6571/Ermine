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

sql = """
SELECT eh.id, eh.date, eh.from, eh.to, eh.cc, eh.subject, eh.pdf_path, eh.has_attachments, eh.attachments_json, ed.body
FROM email_headers eh
JOIN email_data ed ON eh.id = ed.rowid
WHERE ed.body MATCH ?
"""

parameters = [query]  # Using wildcard for partial matches
if sender:
    sql += " AND eh.from = ?"
    parameters.append(f"%{sender}%")

df = pd.read_sql_query(sql, conn, params=parameters)
st.dataframe(df)