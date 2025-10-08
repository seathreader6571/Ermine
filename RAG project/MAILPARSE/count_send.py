import json
import shutil
import re
from pathlib import Path
from collections import Counter

EMAIL_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\Names\Walr")
DEST_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\Names\More than 5")       # destination folder
DEST_DIR.mkdir(parents=True, exist_ok=True)

def count_recipients(msg):
    recipients = []
    for field in ("to", "from" "cc", "bcc"):
        val = msg.get(field)
        if val:
            if isinstance(val, str):
                recipients += [x.strip() for x in val.split(",") if x.strip()]
            elif isinstance(val, list):
                recipients += [x.strip() for x in val if x.strip()]
    n = len(set(recipients))

copied = 0

for file in EMAIL_DIR.glob("*.json"):
    try:
        data = json.loads(file.read_text(encoding="utf-8"))
        n = count_recipients(data)
        if n > 5:
            shutil.copy2(file, DEST_DIR / file.name)
            copied += 1
            print(f"✅ Copied {file.name} ({n} recipients)")
    except Exception as e:
        print(f"⚠️ {file.name}: error - {e}")

print(f"\nDone. {copied} files copied to {DEST_DIR}")