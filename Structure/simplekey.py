import os
import json
import shutil
from pathlib import Path

# === CONFIG ===
PARENT_DIR = Path(r"J:\Ermine\mywritingpad@proton.me\Keywords")      # folder with parsed txt files
DEST_DIR = Path(r"J:\Ermine\mywritingpad@proton.me\Keywords")     # folder where matched files go
KEYWORDS = [" Elia ", " booking ", " hotel room "]  # keywords to search (lowercase)

# Create destination folder if it doesn’t exist
DEST_DIR.mkdir(parents=True, exist_ok=True)

# Walk through all json files
def file_copier(SOURCE_DIR):
    for file in SOURCE_DIR.glob("*.txt"):
        try:
            text = file.read_text(encoding = "utf-8").lower()

            # Check if any keyword matches

            if any(kw in text for kw in KEYWORDS):
                #Preserve folder structure
                rel_path   = file.relative_to(PARENT_DIR)
                dest_path  = DEST_DIR / rel_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)

                shutil.copy2(file, dest_path)
                print(f"✅ Copied: {file.name}")

        except Exception as e:
            print(f"⚠️ Error reading {file}: {e}")

for subdir in PARENT_DIR.iterdir():
    if subdir.is_dir():
        print(f"🔍 Checking in: {subdir.name}")

        # Loop over files in this subdirectory
        file_copier(subdir)