#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import re
from email import policy
from email.parser import BytesParser
from collections import Counter

# Path to the folder containing your .eml files
folder_path = r"C:\Users\drumm\Documents\ERMINE_local\mail_20250910_211624_conversion"

attachment_count = 0
file_count = 0
extensions = Counter()  # To count different file extensions

 # Loop through all files in the folder
for entry in os.scandir(folder_path):
    if entry.name.lower().endswith(".eml") and entry.is_file():
        file_count += 1
        print(f"files processed: {file_count}", end="\r")
        with open(entry.path, "rb") as f:
            msg = BytesParser(policy=policy.default).parse(f)
            for part in msg.iter_attachments():
                attachment_count += 1
                attach_filename = part.get_filename()
                if attach_filename:
                    ext = os.path.splitext(attach_filename)[1].lower()  # Get extension
                        # Only count extensions that are realistic: letters, numbers, 1-5 chars
                    if ext and re.match(r'^\.[a-z0-9]{1,5}$', ext):
                        extensions[ext] += 1
                    else:
                        extensions['[no/unknown]'] += 1

print(f"Total .eml files processed: {file_count}")
print(f"Total attachments found: {attachment_count}\n")

print("Attachment file extensions and counts:")
for ext, count in extensions.most_common():
    print(f"{ext}: {count}")