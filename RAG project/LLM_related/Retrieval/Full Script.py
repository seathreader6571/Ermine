import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"




import argparse
import logging
import torch
from pathlib import Path

from llama_index.core import (
    StorageContext,
    load_index_from_storage,
    PromptTemplate,
)
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.faiss import FaissVectorStore
from llama_index.llms.llama_cpp import LlamaCPP


# --------- Config you should set ----------
PERSIST_DIR = "storage"  # where you persisted the FAISS index
GGUF_PATH = r"Models\llama-2-7b-chat.Q5_K_M.gguf"  # change to your local path
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"  # must match your ingestion
# ------------------------------------------


def pick_offload_layers(vram_gb: float) -> int:
    """
    Heuristic for how many layers to offload to GPU (P1000 has 4 GB).
    If you stick to CPU-only install, this returns 0 and is ignored.
    """
    if vram_gb >= 10:
        return 32      # full offload for 7B
    if vram_gb >= 8:
        return 28
    if vram_gb >= 6:
        return 22
    if vram_gb >= 4:
        return 14      # decent offload for 4 GB VRAM
    return 0


def detect_vram_gb() -> float:
    try:
        if torch.cuda.is_available():
            prop = torch.cuda.get_device_properties(0)
            return prop.total_memory / (1024**3)
    except Exception:
        pass
    return 0.0



def build_llm(args):
    vram_gb = detect_vram_gb()
    n_gpu_layers = args.gpu_layers
    if n_gpu_layers is None:
        n_gpu_layers = pick_offload_layers(vram_gb)

    # Keep context moderate on P1000 to control KV cache memory
    context_window = args.ctx

    # n_threads: use all cores for CPU side; n_batch: batching on GPU/CPU (safe 128–256 for 7B)
    model_kwargs = {
        "n_gpu_layers": n_gpu_layers,
        "n_ctx": context_window,
        "n_threads": max(2, os.cpu_count() or 8),
        "n_batch": args.n_batch,
        "f16_kv": True,          # better KV precision; reduces degradation
        "use_mlock": True,       # lock pages in RAM (Windows: silently ignored)
    }

    llm = LlamaCPP(
        model_path=GGUF_PATH,
        temperature=args.temp,
        max_new_tokens=args.max_new_tokens,
        context_window=context_window,
        model_kwargs=model_kwargs,
        # streaming=True,  # set True if you want token streaming (then print tokens incrementally)
    )
    logging.info(
        f"LLM ready: gguf={os.path.basename(GGUF_PATH)}, "
        f"ctx={context_window}, n_gpu_layers={n_gpu_layers}, n_batch={args.n_batch}"
    )
    return llm



def build_query_engine(llm, top_k: int):
    # Recreate the same embedder used during ingestion for query-time embeddings
    device = "cuda" if torch.cuda.is_available() else "cpu"
    embed_model = HuggingFaceEmbedding(
        model_name=EMBED_MODEL,
        trust_remote_code=True,
        device=device,
        embed_batch_size=64,
    )

    # Load FAISS + docstore from disk
    vector_store = FaissVectorStore.from_persist_dir(PERSIST_DIR)
    storage_context = StorageContext.from_defaults(
        persist_dir=PERSIST_DIR,
        vector_store=vector_store
    )
    index = load_index_from_storage(storage_context, embed_model=embed_model)

    # RAG prompt (grounded, cite sources)
    qa_tmpl = PromptTemplate(
        """You are an assistant for question answering over a collection of emails.
Use ONLY the context to answer. If the answer is not in the context, say you don't know.
When you use information, cite sources as [source: <file or id>].

<context>
{context_str}
</context>

Question: {query_str}

Answer:"""
    )

    # Build query engine
    # similarity_top_k = how many chunks to stuff into the prompt
    qengine = index.as_query_engine(
        llm=llm,
        similarity_top_k=top_k,
        text_qa_template=qa_tmpl,
        response_mode="compact",  # optional: more concise
    )
    return qengine


def print_sources(resp):
    # LlamaIndex Response has .source_nodes with node + score
    used = []
    for sn in getattr(resp, "source_nodes", []) or []:
        meta = sn.node.metadata or {}
        # try common email/pdf metadata keys you saved earlier
        src = (meta.get("source")
               or meta.get("file_path")
               or meta.get("filename")
               or meta.get("doc_id")
               or "unknown")
        used.append((src, round(sn.score or 0.0, 3)))
    if used:
        print("\nSources:")
        for src, score in used:
            print(f" • {src}  (score={score})")


def main():
    parser = argparse.ArgumentParser(description="RAG query over persisted FAISS index + local LLM")
    parser.add_argument("-k", "--top_k", type=int, default=3, help="Top-k chunks to retrieve")
    parser.add_argument("--ctx", type=int, default=4096, help="LLM context window (tokens)")
    parser.add_argument("--temp", type=float, default=0.2, help="LLM temperature")
    parser.add_argument("--max-new-tokens", type=int, default=256, help="Max new tokens to generate")
    parser.add_argument("--n-batch", dest="n_batch", type=int, default=192, help="LLM batch size")
    parser.add_argument("--gpu-layers", type=int, default=None, help="Override number of GPU layers to offload")
    parser.add_argument("--once", action="store_true", help="Run a single prompt and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not os.path.exists(PERSIST_DIR):
        raise SystemExit(f"Persist dir '{PERSIST_DIR}' not found. Did you run ingestion?")

    if not os.path.exists(GGUF_PATH):
        raise SystemExit(f"GGUF model not found at '{GGUF_PATH}'. Set GGUF_PATH in this script.")

    llm = build_llm(args)
    qengine = build_query_engine(llm, top_k=args.top_k)

    def ask_once():
        try:
            q = input("\nAsk a question (blank to exit): ").strip()
        except (EOFError, KeyboardInterrupt):
            q = ""
        if not q:
            return False
        resp = qengine.query(q)
        print(f"\nAnswer:\n{str(resp).strip()}")
        print_sources(resp)
        return True

    if args.once:
        ask_once()
    else:
        print("RAG ready. Ctrl+C to exit.")
        while ask_once():
            pass


if __name__ == "__main__":
    main()