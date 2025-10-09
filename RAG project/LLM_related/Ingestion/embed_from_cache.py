import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # only show TF errors

import json
import logging
from pathlib import Path
from tqdm import tqdm
import torch
import faiss

from llama_index.core import Document, StorageContext, VectorStoreIndex
from llama_index.core.node_parser import SentenceSplitter
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.faiss import FaissVectorStore


# === Logging setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("embedder.log", mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


# === 1. Load cached JSON files into Documents ===
def load_cached_docs(cache_dir: Path):
    docs = []
    for cache_file in tqdm(list(cache_dir.glob("*.json")), desc="Loading cache"):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for d in data:
                docs.append(Document(text=d["text"], metadata=d["metadata"]))
        except Exception as e:
            logging.error(f"❌ Failed loading {cache_file.name}: {e}")
    return docs


# === MAIN ===
if __name__ == "__main__":
    cache_dir = Path("cache")
    if not cache_dir.exists():
        raise RuntimeError("No cache directory found. Please run parsing first.")

    # 1. Load documents
    logging.info("Loading documents from cache...")
    all_docs = load_cached_docs(cache_dir)
    logging.info(f"Loaded {len(all_docs)} documents from {cache_dir}")

    # 2. Split into nodes
    logging.info("Splitting documents into nodes...")
    splitter = SentenceSplitter(chunk_size=2048, chunk_overlap=100)
    nodes = splitter.get_nodes_from_documents(all_docs)
    logging.info(f"Created {len(nodes)} nodes.")

    # 3. Setup embedding model (multi-GPU, FP16)
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA GPU not available. Please run on a machine with GPU.")

    n_gpus = torch.cuda.device_count()
    logging.info(f"Detected {n_gpus} GPU(s).")


    embed_model = HuggingFaceEmbedding(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
            trust_remote_code=True,
            device="cuda",     
            embed_batch_size=64,   # adjust based on VRAM
        )
    

    # 4. Embed nodes
    logging.info("Embedding nodes...")
    texts = [n.get_content() for n in nodes]
    batch_size = 64
    embeddings = []
    
    test_texts = texts[:2]
    logging.info("Testing embedding on 2 nodes...")
    emb_batch = embed_model.get_text_embedding_batch(test_texts)
    logging.info(f"Test batch OK, got {len(emb_batch)} embeddings of dim {len(emb_batch[0])}")



    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding"):
        batch = texts[i:i+batch_size]
        emb_batch = embed_model.get_text_embedding_batch(batch)
        embeddings.extend(emb_batch)

    # Attach embeddings
    for node, emb in zip(nodes, embeddings):
        node.embedding = emb

    logging.info("Finished embeddings.")

    # 5. Build FAISS index
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
    index = VectorStoreIndex(nodes, storage_context=storage_context, embed_model=embed_model)
    index.storage_context.persist(persist_dir="storage")

    logging.info(f"✅ FAISS index built. Contains {faiss_index.ntotal} embeddings.")
    logging.info("📂 Stored in ./storage (docstore.json, index_store.json, vector_store.faiss)")
