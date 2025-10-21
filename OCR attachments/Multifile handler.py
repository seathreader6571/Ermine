# Requirements: pip install easyocr ocrmypdf docx2txt tqdm



# import os
import subprocess
from pathlib import Path
import easyocr
import docx2txt
from tqdm import tqdm

# --- CONFIG ---
INPUT_DIR = Path(r"PATH/TO/INPUT")
OUTPUT_DIR = Path(r"PATH/TO/OUTPUT")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

reader = easyocr.Reader(['en', 'nl', 'es'])  # Add 'nl' or others if needed

def convert_pdf_to_text(file_path: Path, out_txt: Path):
    """Make PDF searchable with OCRmyPDF, then extract text using pdftotext."""
    temp_pdf = OUTPUT_DIR / f"{file_path.stem}_searchable.pdf"
    subprocess.run(["ocrmypdf", "--skip-text", str(file_path), str(temp_pdf)], check=True)
    subprocess.run(["pdftotext", str(temp_pdf), str(out_txt)], check=True)

def convert_docx_to_text(file_path: Path, out_txt: Path):
    text = docx2txt.process(str(file_path))
    out_txt.write_text(text, encoding="utf-8")

def convert_image_to_text(file_path: Path, out_txt: Path):
    result = reader.readtext(str(file_path), detail=0, paragraph=True)
    text = "\n".join(result)
    out_txt.write_text(text, encoding="utf-8")

def process_file(file_path: Path):
    ext = file_path.suffix.lower()
    out_txt = OUTPUT_DIR / f"{file_path.stem}.txt"

    try:
        if ext == ".pdf":
            convert_pdf_to_text(file_path, out_txt)
        elif ext in [".doc", ".docx"]:
            convert_docx_to_text(file_path, out_txt)
        elif ext in [".jpg", ".jpeg", ".png"]:
            convert_image_to_text(file_path, out_txt)
        else:
            print(f"Skipping unsupported file: {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    files = [f for f in INPUT_DIR.glob("**/*") if f.is_file()]
    for f in tqdm(files, desc="Processing files"):
        process_file(f)

if __name__ == "__main__":
    main()
