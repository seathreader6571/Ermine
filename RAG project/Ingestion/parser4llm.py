#Import required libraries
import pymupdf4llm
from pathlib import Path
import faiss
from llama_index.core import Document
from llama_index.core import StorageContext
from llama_index.core.node_parser import SentenceSplitter
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.faiss import FaissVectorStore
from llama_index.core import VectorStoreIndex
#Define file +convert to markdown
file_path = Path(r"E:\Ermine\250213 ATTACHMENTS\Bijlage 01A - Opinie BDO over de waarde van één aandeel SN Industries d.d. 21 maart 2024(103661691.1).pdf")
md_text = pymupdf4llm.to_markdown(file_path)


# Wrap into a LlamaIndex Document
doc = Document(text=md_text, metadata={"source": str(file_path)})

#Split into nodes(512 tokens)
splitter = SentenceSplitter(chunk_size=512, chunk_overlap=50)
nodes = splitter.get_nodes_from_documents([doc])



#Specify embedding model
embed_model = HuggingFaceEmbedding(model_name="nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True)

# Create FAISS index manually (dimension must match embedding size, e.g. 768 or 1536)
dimension = 768  # check your embedding model's output size
faiss_index = faiss.IndexFlatL2(dimension)


# Save raw FAISS index to disk
faiss.write_index(faiss_index, "storage/vector_store.faiss")

faiss_index = faiss.read_index("storage/vector_store.faiss")
# Wrap it into a FaissVectorStore
vector_store = FaissVectorStore(faiss_index=faiss_index)

# Create storage context with persist_dir
storage_context = StorageContext.from_defaults(vector_store=vector_store)

# Save everything (index + metadata)

index = VectorStoreIndex(nodes, storage_context=storage_context, embed_model=embed_model)
index.storage_context.persist(persist_dir="storage")

## Choose where to save the chunks
#output_file = Path("nodes_output.txt")
#
#with output_file.open("w", encoding="utf-8") as f:
#    for i, node in enumerate(nodes, start=1):
#        f.write(f"--- Node {i} ---\n")
#        f.write(node.text.strip() + "\n")
#        f.write(f"Metadata: {node.metadata}\n")
#        f.write("\n\n")
#
#print(f"Saved {len(nodes)} nodes to {output_file.resolve()}")