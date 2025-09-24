import fitz
from pathlib import Path

def pdf_to_markdown(path: str) -> str:
    """
    Parse a PDF into structured Markdown using PyMuPDF (fitz).
    Keeps headings, articles, lists, and paragraphs.
    """

    md_output = []
    doc = fitz.open(path)

    for page in doc:
        blocks = page.get_text("blocks")  # list of text blocks on the page
        # Sort by y0 (vertical position) to preserve reading order
        blocks = sorted(blocks, key=lambda b: (b[1], b[0]))

        for b in blocks:
            text = b[4].strip()
            if not text:
                continue

            # ---- Heuristic rules for Markdown formatting ----
            if text.isupper() and len(text.split()) < 10:
                # Likely a title or section heading
                md_output.append(f"# {text}")

            elif text.lower().startswith("artikel"):
                # Legal article headings
                md_output.append(f"## {text}")

            elif text.startswith(("•", "-", "–", "—")):
                # Bulleted list item
                md_output.append(f"- {text.lstrip('•–—- ').strip()}")

            elif text[0].isdigit() and text[1:3] in [". ", ") "]:
                # Numbered list (e.g., "1. ..." or "2) ...")
                md_output.append(f"1. {text[2:].strip()}")

            else:
                # Default: treat as paragraph
                md_output.append(text)

    doc.close()
    return "\n\n".join(md_output)

file_path = Path(r"E:\Ermine\250213 ATTACHMENTS\Bijlage 01A - Opinie BDO over de waarde van één aandeel SN Industries d.d. 21 maart 2024(103661691.1).pdf"
)
print(pdf_to_markdown(file_path))