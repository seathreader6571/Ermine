import os
import json
import shutil
from pathlib import Path

# === CONFIG ===
PARENT_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\output_txt\emails")      # folder with parsed txt files
DEST_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\Travel")     # folder where matched files go
NAME = "uber"  # keywords to search (lowercase)

# Create destination folder if it doesn’t exist
DEST_DIR.mkdir(parents=True, exist_ok=True)




def detect_name(data: dict, name: str) -> bool:
    name = name.lower()
    for key in ['from']:
        value = data.get(key, "")
        if isinstance(value, str) and name in value.lower():
            return True
        elif isinstance(value, list) and any(name in str(v).lower() for v in value):
            return True
        elif isinstance(value, dict):
            return detect_name(value) 
    return False

# Walk through all json files
def file_copier(SOURCE_DIR, name = NAME):
    for file in SOURCE_DIR.glob("*.json"):
        try:
            with file.open(encoding="utf-8") as f:
                js = json.load(f)

            if detect_name(js, name):
                rel_path   = file.relative_to(PARENT_DIR)
                dest_path  = DEST_DIR / NAME / rel_path 
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



if __name__ == "__main__":
    file_copier(PARENT_DIR)