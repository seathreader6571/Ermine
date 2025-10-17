import os
import json
import shutil
from pathlib import Path

PARENT_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\output_txt\emails")      # folder with parsed txt files
DEST_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\Names\Only from")     # folder where matched files go


#---------------------------------------------------------------------------------------------------------------------------------------------------
# This script counts how many Names appear in 'from', 'to' or 'cc'. NOT ACCURATE!!! MERELY AN UNDERBOUND FOR THE NUMBER OF PEOPLE IN A SINGLE THREAD
#---------------------------------------------------------------------------------------------------------------------------------------------------

def count_people(dit: dict) -> int:
    target_keys = ['from', 'to', 'cc']
    for ks in list(dit.keys()):
        if ks in target_keys:


# Walk through all json files
def file_copier(SOURCE_DIR):
    for file in SOURCE_DIR.glob("*.json"):
        try:
            with file.open(encoding="utf-8") as f:
                js = json.load(f)

            if count_people(js):
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



if __name__ == "__main__":
    file_copier(PARENT_DIR)