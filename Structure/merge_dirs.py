import os
import shutil
from pathlib import Path

def merge_directories(dir1, dir2, target_dir):
    """
    Merge files from dir1 and dir2 into target_dir, avoiding duplicate filenames.
    Keeps the first occurrence of each filename.
    """
    # Ensure source directories exist
    if not os.path.isdir(dir1) or not os.path.isdir(dir2):
        print("❌ Error: One or both source directories do not exist.")
        return

    # Create target directory if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)

    # To track filenames already moved
    seen_filenames = set()

    def move_unique_files(source):
        for filename in os.listdir(source):
            source_path = os.path.join(source, filename)
            target_path = os.path.join(target_dir, filename)

            # Skip directories
            if os.path.isdir(source_path):
                continue

            # Check duplicates
            if filename not in seen_filenames:
                shutil.move(source_path, target_path)
                seen_filenames.add(filename)
                print(f"✅ Moved: {filename}")
            else:
                print(f"⚠️ Skipped duplicate: {filename}")

    # Process both directories
    move_unique_files(dir1)
    move_unique_files(dir2)

    print("\n🎉 Merge complete! Files saved in:", target_dir)


# ====== 🔧 CONFIGURE YOUR DIRECTORIES HERE ======
DIR1 = Path(r"A:\Ermine\mywritingpad@proton.me\Names")
DIR2 = Path(r"A:\Ermine\mywritingpad@proton.me\Only from")
TARGET_DIR = Path(r"A:\Ermine\mywritingpad@proton.me\Walr")
# ===============================================

if __name__ == "__main__":
    merge_directories(DIR1, DIR2, TARGET_DIR)
