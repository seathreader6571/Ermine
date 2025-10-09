from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.faiss import FaissVectorStore

import torch

PERSIST_DIR = "storage"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

print("🔍 Testing storage load from:", PERSIST_DIR)

try:
    device = "cuda" if torch.cuda.is_available() else "cpu"
    embed_model = HuggingFaceEmbedding(model_name=EMBED_MODEL, device=device)

    vector_store = FaissVectorStore.from_persist_dir(PERSIST_DIR)
    storage_context = StorageContext.from_defaults(
        persist_dir=PERSIST_DIR,
        vector_store=vector_store
    )

    index = load_index_from_storage(storage_context, embed_model=embed_model)
    print("✅ Successfully loaded index!")
    print(f"Documents in index: {len(index.docstore.docs)}")

except Exception as e:
    print("❌ Failed to load index:", type(e).__name__, str(e))

import sys
print("Script finished.")
sys.exit(0)