import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # only show errors



import re
import json
import shutil
import logging
from pathlib import Path
from tqdm import tqdm
import fitz
import pymupdf4llm
import concurrent.futures

from llama_index.core import Document
from llama_index.core.node_parser import SentenceSplitter

fitz.TOOLS.mupdf_display_errors(False)

# === Logging setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("parser.log", mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


# === 1. Helper: split emails into documents ===
def split_emails(md_text, file_path):
    email_splits = re.split(r"\n(?:From|Van): ", md_text)
    docs = []
    for i, email in enumerate(email_splits):
        email_text = email.strip()
        if not email_text:
            continue
        if not (email_text.startswith("From:") or email_text.startswith("Van:")):
            email_text = "From: " + email_text
        docs.append(
            Document(
                text=email_text,
                metadata={
                    "source": str(file_path.name),
                    "thread_id": str(file_path.stem),
                    "email_id": i
                }
            )
        )
    return docs


# === 2. PDF parsing with caching (optimized safe, with timeout) ===
def parse_and_cache(file_path, cache_dir, failed_dir, timeout=20):
    cache_file = cache_dir / (file_path.stem + ".json")

    # Try loading from cache
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return [Document(text=d["text"], metadata=d["metadata"]) for d in data]
        except Exception as e:
            tqdm.write(f"⚠️ Corrupt cache for {file_path.name}, re-parsing: {e}")

    try:
        # Run parsing in a thread with timeout
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(pymupdf4llm.to_markdown, file_path)
            md_text = future.result(timeout=timeout)

        docs = split_emails(md_text, file_path)

        # Save to cache
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump([{"text": d.text, "metadata": d.metadata} for d in docs], f)

        return docs

    except concurrent.futures.TimeoutError:
        tqdm.write(f"⏱ Timeout parsing {file_path.name}, moving to failed_pdfs/")
    except Exception as e:
        tqdm.write(f"❌ Failed parsing {file_path.name}: {e}")

    # Handle failed files
    try:
        shutil.copy(file_path, failed_dir / file_path.name)
        tqdm.write(f"📂 Copied bad file to {failed_dir}/{file_path.name}")
    except Exception as copy_err:
        tqdm.write(f"Could not copy {file_path.name}: {copy_err}")
    return None


# === Wrapper for multiprocessing ===
def parse_task(args):
    file_path, cache_dir, failed_dir = args
    return parse_and_cache(file_path, cache_dir, failed_dir)


# === MAIN ===
if __name__ == "__main__":
    print("🚀 Starting PDF parsing + splitting pipeline...")
    folder_path = Path(r"J:\Ermine\mywritingpad@proton.me\output")       # where your PDFs are
    cache_dir = Path("cache")
    failed_dir = Path("failed_pdfs")

    cache_dir.mkdir(exist_ok=True)
    failed_dir.mkdir(exist_ok=True)

    pdf_files = list(folder_path.rglob("*.pdf"))
    logging.info(f"Found {len(pdf_files)} PDF files in folder.")

    all_docs = [] 
    failed_files = []
    tasks = [(f, cache_dir, failed_dir) for f in pdf_files]

    # Parse in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(tqdm(
            executor.map(parse_task, tasks),
            total=len(pdf_files),
            desc="Parsing PDFs (with cache)"
        ))

    for file_path, docs in zip(pdf_files, results):
        if docs is not None:
            all_docs.extend(docs)
        else:
            failed_files.append(file_path.name)

    logging.info(f"✅ Successfully parsed {len(pdf_files) - len(failed_files)}/{len(pdf_files)} PDFs")    
    logging.info(f"❌ Failed parsing {len(failed_files)} PDFs")

    if failed_files:
        with open(failed_dir / "failed_files.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(failed_files))

    # === Split into nodes ===
    logging.info("Splitting emails into nodes...")
    splitter = SentenceSplitter(chunk_size=2048, chunk_overlap=100)
    nodes = splitter.get_nodes_from_documents(all_docs)
    logging.info(f"Created {len(nodes)} nodes.")

    # Example: save nodes to JSON for inspection
    with open("nodes.json", "w", encoding="utf-8") as f:
        json.dump(
            [{"text": n.get_content(), "metadata": n.metadata} for n in nodes],
            f,
            ensure_ascii=False,
            indent=2
        )

    logging.info("Nodes saved to nodes.json")
 