import os
import glob
import json
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
import hdbscan
from sklearn.metrics import pairwise_distances_argmin_min

# --- config ---
EMAIL_DIR = "cache"   # folder with your JSON files
CSV_OUT = "email_clusters.csv"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# --- load model ---
embedder = SentenceTransformer(MODEL_NAME)

# --- collect emails ---
emails = []
for file in glob.glob(os.path.join(EMAIL_DIR, "*.json")):
    with open(file, "r", encoding="utf-8", errors="ignore") as f:
        data = json.load(f)

        if isinstance(data, list):
            for i, item in enumerate(data):
                if "text" in item and item["text"].strip():
                    emails.append((f"{os.path.basename(file)}#{i}", item["text"].strip()))
        elif isinstance(data, dict):
            if "text" in data and data["text"].strip():
                emails.append((os.path.basename(file), data["text"].strip()))

print(f"Found {len(emails)} JSON records with 'text'.") 

#for file in glob.glob(os.path.join(EMAIL_DIR, "*.json")):
#    try:
#        with open(file, "r", encoding="utf-8", errors="ignore") as f:
#            data = json.load(f)
#            if "text" in data and data["text"].strip():
#                emails.append((os.path.basename(file), data["text"].strip()))
#    except Exception as e:
#        print(f"[!] Skipping {file}: {e}")

print(f"Found {len(emails)} JSON files with 'text'.")

if not emails:
    raise RuntimeError("No usable JSON files found in EMAIL_DIR.")

# --- embed ---
texts = [t for _, t in emails]
embeddings = embedder.encode(
    texts, convert_to_numpy=True, show_progress_bar=True
)

# normalize embeddings row-wise
embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

# then cluster with Euclidean
clusterer = hdbscan.HDBSCAN(min_cluster_size=5, metric="euclidean")
labels = clusterer.fit_predict(embeddings)

# --- representatives per cluster ---
unique_labels = sorted(set(labels) - {-1})
representatives = []
for cluster_id in unique_labels:
    idxs = np.where(labels == cluster_id)[0]
    centroid = embeddings[idxs].mean(axis=0).reshape(1, -1)
    closest, _ = pairwise_distances_argmin_min(centroid, embeddings[idxs])
    representatives.append(emails[idxs[closest[0]]])

# --- save to CSV ---
df = pd.DataFrame({
    "filename": [fn for fn, _ in emails],
    "text": texts,
    "cluster": labels
})

# Save one CSV per cluster (excluding noise, which is cluster = -1)
for cluster_id, group in df.groupby("cluster"):
    if cluster_id == -1:
        out_file = f"cluster_noise.csv"
    else:
        out_file = f"cluster_{cluster_id}.csv"
    group.to_csv(out_file, index=False)

print(f"[✓] Wrote {len(df['cluster'].unique())} CSV files (one per cluster).")


print(f"[✓] Clustered {len(emails)} emails into {len(unique_labels)} groups (+ noise).")
print("Representative emails per cluster:")
for i, (fname, text) in enumerate(representatives):
    print(f"Cluster {i}: {fname} → {text[:120]}...")
