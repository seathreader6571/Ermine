# -----------------------------
# Forwarded message detection (cluster + fallback + safe recursion)
# -----------------------------
HEADER_KEYWORDS = {
    "English": ("To:", "Cc:", "Date:", "Subject:", "Sent:"),
    "Dutch": ("Aan:", "Cc:", "Onderwerp:", "Datum:", "Verzonden:")
}

HEADER_ALIASES = {
    "from": "from", "van": "from",
    "to": "to", "aan": "to",
    "cc": "cc",
    "bcc": "bcc",
    "subject": "subject", "onderwerp": "subject",
    "date": "date", "datum": "date", "sent": "date", "verzonden": "date",
}

EXPLICIT_FORWARD_MARKERS = [
    "Forwarded message",
    "Original Message",
    "Begin forwarded message",
]

MAX_DEPTH = 15


def normalize_header_line(line: str) -> str:
    line = line.lstrip()
    line = re.sub(r"\s*:\s*", ":", line, count=1)
    return line


def is_forward_marker(line: str) -> bool:
    return any(marker.lower() in line.lower() for marker in EXPLICIT_FORWARD_MARKERS)


def header_language(body: str):
    """Determine whether headers are in English or Dutch based on first occurrence."""
    f_idx = body.lower().find("from:")
    v_idx = body.lower().find("van:")
    if f_idx == -1 and v_idx == -1:
        return None
    if v_idx == -1 or (0 <= f_idx < v_idx):
        return "English"
    return "Dutch"


def add_fwd_key(first_dict, fwd_block) -> dict:
    first_dict['fwd'] = fwd_block
    return(first_dict)

def first_split(msg_dict: dict):
    """Split the body at the first 'From:' or 'Van:' and return the updated dictionary"""
    body = msg_dict.get("body", "")
    lower_body = body.lower()

    # Find earliest header occurrence
    from_idx = lower_body.find("from:")
    van_idx = lower_body.find("van:")
    if from_idx == -1 and van_idx == -1:
        # nothing to split
        return msg_dict, None

    split_idx = min(x for x in [from_idx, van_idx] if x != -1)

    # Split into main and forwarded
    old_body = body[:split_idx].strip()
    new_body = body[split_idx:].strip()

    # Update main dict
    msg_dict["body"] = old_body

    # Create forwarded dict
    forwarded_dict = {
        "from": None, 
        "to": None,
        "cc": None,
        "subject": None,
        "date": None,
        "body": new_body,
        "fwd": None, 
    }

    return add_fwd_key(msg_dict, forwarded_dict)