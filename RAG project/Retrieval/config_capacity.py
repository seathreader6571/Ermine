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
