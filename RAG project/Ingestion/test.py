import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # only show errors

import re
import json
import shutil
import logging
from pathlib import Path
from tqdm import tqdm
import torch
import faiss
import fitz
import pymupdf4llm
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures

from llama_index.core import Document, StorageContext, VectorStoreIndex
from llama_index.core.node_parser import SentenceSplitter
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.faiss import FaissVectorStore

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
    """
    Try parsing PDF with pymupdf4llm.
    If parsing takes longer than `timeout` seconds, mark file as failed.
    """
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
            md_text = future.result(timeout=timeout)   # enforce timeout

        docs = split_emails(md_text, file_path)

        # Save to cache
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump([{"text": d.text, "metadata": d.metadata} for d in docs], f)

        return docs

    except concurrent.futures.TimeoutError:
        tqdm.write(f"⏱ Timeout parsing {file_path.name}, moving to failed_pdfs/")
    except Exception as e:
        tqdm.write(f"❌ Failed parsing {file_path.name}: {e}")

    # Handle failed files (timeout or error)
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
    folder_path = Path(r"D:\mywritingpad@proton.me\output")   # <-- set your PDF folder
    cache_dir = Path("cache")
    failed_dir = Path("failed_pdfs")

    cache_dir.mkdir(exist_ok=True)
    failed_dir.mkdir(exist_ok=True)

    pdf_files = list(folder_path.rglob("*.pdf"))
    logging.info(f"Found {len(pdf_files)} PDF files in folder.")

    # === 1. Parse all PDFs in parallel ===
    all_docs = []
    failed_files = []
    tasks = [(f, cache_dir, failed_dir) for f in pdf_files]

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
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

    success_count = len(pdf_files) - len(failed_files)
    fail_count = len(failed_files)
    logging.info(f"✅ Successfully parsed {success_count}/{len(pdf_files)} PDFs")
    logging.info(f"❌ Failed parsing {fail_count} PDFs (moved to {failed_dir})")

    if failed_files:
        failed_list_path = failed_dir / "failed_files.txt"
        with open(failed_list_path, "w", encoding="utf-8") as f:
            f.write("\n".join(failed_files))
        logging.info(f"📄 List of failed files written to {failed_list_path}")

    # === 2. Split into nodes ===
    logging.info("Splitting emails into nodes...")
    splitter = SentenceSplitter(chunk_size=2048, chunk_overlap=100)
    nodes = splitter.get_nodes_from_documents(all_docs)
    logging.info(f"Created {len(nodes)} nodes.")

    # === 3. Embedding model (multi-GPU, FP16) ===
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA GPU not available. Please run on a machine with GPU.")

    n_gpus = torch.cuda.device_count()
    logging.info(f"Detected {n_gpus} GPU(s).")

    devices = [f"cuda:{i}" for i in range(n_gpus)]
    embed_models = [
        HuggingFaceEmbedding(
            model_name="nomic-ai/nomic-embed-text-v1.5",
            trust_remote_code=True,
            device=dev,
            embed_batch_size=128,
            dtype="float16"
        )
        for dev in devices
    ]

    logging.info("Embedding nodes across GPUs...")
    texts = [n.get_content() for n in nodes]
    batch_size = 128
    embeddings = []

    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding"):
        batch = texts[i:i+batch_size]
        model = embed_models[(i // batch_size) % len(embed_models)]
        emb_batch = model.get_text_embedding_batch(batch)
        embeddings.extend(emb_batch)

    for node, emb in zip(nodes, embeddings):
        node.embedding = emb

    logging.info("Finished embeddings.")

    # === 4. FAISS index (GPU if available) ===
    dimension = len(embeddings[0])
    cpu_index = faiss.IndexFlatL2(dimension)

    if hasattr(faiss, "StandardGpuResources"):
        res = faiss.StandardGpuResources()
        faiss_index = faiss.index_cpu_to_gpu(res, 0, cpu_index)
        logging.info("Using GPU FAISS index.")
    else:
        faiss_index = cpu_index
        logging.info("Using CPU FAISS index.")

    vector_store = FaissVectorStore(faiss_index=faiss_index)
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    logging.info("Building FAISS index and persisting storage...")
    index = VectorStoreIndex(nodes, storage_context=storage_context, embed_model=embed_models[0])
    index.storage_context.persist(persist_dir="storage")

    logging.info(f"FAISS index built. Contains {faiss_index.ntotal} embeddings.")
    logging.info("Stored in ./storage (docstore.json, index_store.json, vector_store.faiss)")

    # === Final summary ===
    logging.info("=== SUMMARY ===")
    logging.info(f"📊 Total PDFs: {len(pdf_files)}")
    logging.info(f"✅ Parsed successfully: {success_count}")
    logging.info(f"❌ Failed: {fail_count} (see {failed_dir}/failed_files.txt)")
    logging.info(f"📂 Cache: {cache_dir}")
    logging.info(f"📂 Storage: ./storage")
