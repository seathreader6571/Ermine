import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # only show errors, no info/warnings

import time
import random
import re
import json
import pymupdf4llm
from pathlib import Path
import faiss
from tqdm import tqdm
import torch
from concurrent.futures import ProcessPoolExecutor

from llama_index.core import Document, StorageContext, VectorStoreIndex
from llama_index.core.node_parser import SentenceSplitter
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.faiss import FaissVectorStore





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


# === 2. PDF parsing with caching ===
def parse_and_cache(file_path, cache_dir):
    cache_file = cache_dir / (file_path.stem + ".json")

    # If already cached, load from JSON
    if cache_file.exists():
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return [Document(text=d["text"], metadata=d["metadata"]) for d in data]

    # Otherwise parse fresh
    md_text = pymupdf4llm.to_markdown(file_path)
    docs = split_emails(md_text, file_path)

    # Save to cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump([{"text": d.text, "metadata": d.metadata} for d in docs], f)

    return docs


# === 3. Wrapper for executor (must be top-level, no lambdas) ===
def parse_wrapper(file_path_and_cache):
    file_path, cache_dir = file_path_and_cache
    return parse_and_cache(file_path, cache_dir)



def profile_parsing(pdf_files, cache_dir, sample_size=5):
    if not pdf_files:
        print("⚠️ No PDFs to profile.")
        return

    sample = random.sample(pdf_files, min(sample_size, len(pdf_files)))
    io_times, parse_times = [], []

    for f in sample:
        print(f"Profiling {f.name}...")
        
        # --- I/O timing ---
        start = time.time()
        with open(f, "rb") as fh:
            data = fh.read()
        io_time = time.time() - start
        io_times.append(io_time)

        # --- parsing timing ---
        start = time.time()
        _ = pymupdf4llm.to_markdown(f)
        parse_time = time.time() - start
        parse_times.append(parse_time)

        print(f"  I/O: {io_time:.3f}s, Parse: {parse_time:.3f}s, Size: {len(data)/1e6:.1f}MB")

    print("\n=== Profiling Summary ===")
    print(f"Avg I/O time   : {sum(io_times)/len(io_times):.3f}s")
    print(f"Avg Parse time : {sum(parse_times)/len(parse_times):.3f}s")
    if sum(parse_times) > sum(io_times) * 2:
        print("➡️ Mostly CPU-bound (parsing dominates).")
    elif sum(io_times) > sum(parse_times) * 2:
        print("➡️ Mostly I/O-bound (disk/network dominates).")
    else:
        print("➡️ Mixed workload (both I/O and CPU matter).")



# === MAIN ===
if __name__ == "__main__":
    folder_path = Path(r"D:\mywritingpad@proton.me\output")   # <-- set your PDF folder
    cache_dir = Path("cache")
    cache_dir.mkdir(exist_ok=True)

    pdf_files = list(folder_path.glob("*.pdf"))
    print(f"Found {len(pdf_files)} PDF files in folder.")

    # --- profiling run ---
    profile_parsing(pdf_files, cache_dir, sample_size=5)
    exit()  # stop here, no embeddings / FAISS

    all_docs = []
    with ProcessPoolExecutor() as executor:
        tasks = [(f, cache_dir) for f in pdf_files]  # bundle args
        results = list(tqdm(
            executor.map(parse_wrapper, tasks),
            total=len(pdf_files),
            desc="Parsing PDFs (with cache)"
        ))
        for docs in results:
            all_docs.extend(docs)

    print(f"✅ Extracted {len(all_docs)} emails.")

    # === 4. Chunk into nodes ===
    print("🔹 Splitting emails into nodes...")
    splitter = SentenceSplitter(chunk_size=1024, chunk_overlap=100)
    nodes = splitter.get_nodes_from_documents(all_docs)
    print(f"✅ Created {len(nodes)} nodes.")

    # === 5. Embedding model (GPU if available) ===
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"🔹 Loading embedding model on: {device}")
    embed_model = HuggingFaceEmbedding(
        model_name="nomic-ai/nomic-embed-text-v1.5",
        trust_remote_code=True,
        device=device
    )

    # === 6. Batch embed nodes ===
    print("🔹 Embedding nodes...")
    texts = [n.get_content() for n in nodes]
    batch_size = 32
    embeddings = []

    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding"):
        batch = texts[i:i+batch_size]
        emb_batch = embed_model.get_text_embedding_batch(batch)
        embeddings.extend(emb_batch)

    for node, emb in zip(nodes, embeddings):
        node.embedding = emb

    print("✅ Finished embeddings.")

    # === 7. Create FAISS index ===
    dimension = len(embeddings[0])
    faiss_index = faiss.IndexFlatL2(dimension)

    vector_store = FaissVectorStore(faiss_index=faiss_index)
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    print("🔹 Building FAISS index and persisting storage...")
    index = VectorStoreIndex(nodes, storage_context=storage_context, embed_model=embed_model)
    index.storage_context.persist(persist_dir="storage")

    print(f"✅ FAISS index built. Contains {faiss_index.ntotal} embeddings.")
    print("📂 Stored in ./storage (docstore.json, index_store.json, vector_store.faiss)")
