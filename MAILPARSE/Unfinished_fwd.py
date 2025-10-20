#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from tqdm import tqdm
import pandas as pd


# -----------------------------
# CONFIG
# -----------------------------
INPUT_DIR_seathreader = Path(r"E:\Ermine\mywritingpad@proton.me\Testing\Input")
OUTPUT_DIR_seathreader = Path(r"E:\Ermine\mywritingpad@proton.me\Testing\Output")

INPUT_DIR_drummingsnipe = Path(r"C:/Users/drumm/Documents/ERMINE (deprecated)/testbatch/output_json")
OUTPUT_DIR_drummingsnipe = Path(r"C:/Users/drumm/Documents/ERMINE (deprecated)/testbatch/splitted_json")

OUTPUT_DIR_seathreader.mkdir(parents=True, exist_ok=True)
MAX_DEPTH = 35
DEBUG = True  # set to False to quiet the logs

FORWARD_MARKERS = (
    "Begin forwarded message",
    "Forwarded message",
    "Original Message",
    "op"
)

# localized label → canonical key
LABEL_TO_CANON = {
    "from:": "from:", "van:": "from:", "de:": "from:",
    "to:": "to:", "aan:": "to:", "para:": "to:",
    "cc:": "cc:",
    "bcc:": "bcc:",
    "subject:": "subject:", "onderwerp:": "subject:", "asunto:": "subject:",
    "date:": "date:", "datum:": "date:", "sent:": "date:", "verzonden:": "date:", "enviado el:": "date:", "fecha:": "date:"
}

HEADER_KEYWORDS = {
    'Dutch': ('aan:', 'onderwerp:', 'datum:', 'verzonden:', 'cc:'),
    'English': ('to:', 'subject:', 'date:', 'sent:', 'cc:')
}
HEADER_RE = re.compile(r"\s?(from|van|to|aan|cc|bcc|subject|onderwerp|date|datum|sent|verzonden|asunto|enviado el|de|para|fecha):")
EMAIL_ADRESS_RE = re.compile(r'\n?[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+\n?>')
SUBJECT_RE = re.compile(r'(subject|asunto|onderwerp):(\n)?[^\n]+\n', re.DOTALL)
SPECIAL_HEADER_RE_EN = re.compile(r'\nOn.{0,20}?at.{0,40}?@[a-zA-Z0-9-]+\.[a-zA-Z0-9-\s]+>?[\s]*wrote:', re.DOTALL)
SPECIAL_HEADER_RE_NL = re.compile(r"\nOp.{0,25}?heeft.+\n?[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+\n?> het volgende geschreven:")
SPECIAL_HEADER_RE_ES = re.compile(r"\nEl.{0,14}, a las .{0,10}?, .+?<\n?[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+.+?escribió:")
#-------------------------------------
# HELPER-FUNCTIONS
#-------------------------------------

def normalize(text: str) -> str:
    """Normalize newlines and strip odd characters & literal escapes."""
    if text is None:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n")
    text = text.replace("/n", "\n")
    text = text.replace("\uFEFF", "")
    return text

def change_key_format(msg_dict: dict) -> dict:
    new_dict = {f"{key}:": v for key, v in msg_dict.items()} 
    return(new_dict)

def transform_email(text: str) -> str:
    w = re.search(r'\n?[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+\n??', text)
    text = text[:w.end()+1] + ">" + text[w.end()+1:]
    return text



def transform_headers(msg_dict: dict) -> dict:
    body = normalize(msg_dict['body:'])
    v = SPECIAL_HEADER_RE_EN.search(body)
    nl = SPECIAL_HEADER_RE_NL.search(body)
    es = SPECIAL_HEADER_RE_ES.search(body)

    if v is not None:
        print(body[v.start(): v.end()])
        part = body[v.start(): v.end()]
        part = re.sub(r"\nOn", "\nDate:", part)
        part = transform_email(part)
        res = re.search(r'at .{0,15}?,', part)   
        part = part[:res.end()] + "\nFrom:" + part[res.end()+ 1:]
        print(part)
        body = body[:v.start()] + part + body[v.end():]
        print(body[v.start(): v.end()+3])
        print(body[v.start(): v.end()+20])
        msg_dict["body:"] = body
        return transform_headers(msg_dict)

    elif nl is not None:
        print(body[nl.start(): nl.end()])
        part = body[nl.start(): nl.end()]
        part = re.sub(r"\nOp", "\nDate:", part)
        part = re.sub(r' heeft', " \nFrom:", part)
        part = transform_email(part)
        part = re.sub(r' het volgende geschreven:', " msg:", part)
        print(part)
        body = body[:nl.start()] + part + body[nl.end():]
        print(body[nl.start(): nl.end()+3])
        print(body[nl.start(): nl.end()+20])
        msg_dict["body:"] = body
        return transform_headers(msg_dict)

    elif es is not None:
        print(body[es.start(): es.end()])
        part = body[es.start(): es.end()]
        part = re.sub(r"\nEl", "\nDate:", part)
        part = transform_email(part)
        res = re.search(r'a las .{0,15}?,', part) 
        part = part[:res.end()] + "\nFrom:" + part[res.end()+ 1:]
        part = re.sub(r'escribió:', " msg:", part)
        print(part)
        body = body[:nl.start()] + part + body[nl.end():]
        print(body[nl.start(): nl.end()+3])
        print(body[nl.start(): nl.end()+20])
        msg_dict["body:"] = body
        return transform_headers(msg_dict)

    return msg_dict



def keyword_to_language(kw: str)-> str:
    if kw == 'van:':
        return('Dutch')
    if kw == 'from:':
        return('English')
    else:
        print('No proper header start, something wrong')


def list_merge(ls: list) -> str:
    delim = "\n"
    return(delim.join(ele for ele in ls))



# This function takes a body of text, from which we know that the first 'line' starts witb a header keyword,
#  and returns the keyword together with the text - keyword.
def extract_header_word(text: str):
    split = text.casefold().splitlines()
    line = split[0]
    v = HEADER_RE.search(line)
    header_word = text[v.start(): v.end()]
    rest_of_text = text[v.end(): ]
    return header_word.casefold(), rest_of_text



#Function below detects a header cluster by listing the header key words untill a duplicate is found.

def header_cluster(text: str) -> bool:
    first, rest = extract_header_word(text)
    splitted = rest.splitlines()
    headerlist = [LABEL_TO_CANON[first]]
    for i, ln in enumerate(splitted):
        lin = ln.strip().casefold()
        key = next((k for k in LABEL_TO_CANON.keys() if lin.startswith(k)), None)
        if key:
            if LABEL_TO_CANON[key] in headerlist:
                # same header keyword → new cluster begins → stop before it
                return i -1
            headerlist.append(LABEL_TO_CANON[key])
            #print(headerlist)


    # no duplicates found → cluster runs until the last line
    return len(splitted) - 1



#THe function below takes the dictionary and the 'body' text and splits it recursively to a new dictionary for every header start occurence.

def first_body_split(upper_dict: dict) -> dict:
    body = normalize(upper_dict['body:']).strip()
    splitbody = body.splitlines()

    first_head = next(
        (i for i, ln in enumerate(splitbody)
         if any(ln.casefold().startswith(k) for k in LABEL_TO_CANON.keys())),
         None
    )


    if first_head is None:
        print("No forward header found")
        return(upper_dict)
    
    upper_dict['body:'] = "\n".join(splitbody[:first_head])

    new_body = "\n".join(splitbody[first_head:])

    new_dict = {  
            "from:": "Name",
            "to:": "Name",
            "cc:": "Name",
            "subject:": "some text",
            "date:": "some date",
            "body:": new_body
    }

    upper_dict['fwd_mess'] = new_dict


    def recursive_body_split(MSG_dict: dict, depth=0, max_depth=MAX_DEPTH):
        BODY = normalize(MSG_dict['body:']).strip()
        SPLITBODY = BODY.splitlines()
        #print([SPLITBODY[:2]])
        
        # Find start of the first header cluster
        fwd_start = next(
            (i for i, ln in enumerate(SPLITBODY)
                if any(ln.casefold().startswith(k) for k in LABEL_TO_CANON.keys())),
            None
        )

        if fwd_start is None or depth >= max_depth:
            print("end of fwd")
            return MSG_dict  # base case — no more headers or too deep

        # --- Detect where this header cluster ends ---
        #print(splitted[fwd_start])
        cluster_text = "\n".join(SPLITBODY[fwd_start:])      # pass rest of text
        cluster_end_rel = header_cluster(cluster_text)        # index *within* cluster_text
        cluster_end = fwd_start + cluster_end_rel             # absolute index in splitted
        #print(SPLITBODY[:cluster_end])
        # --- Split text based on that cluster ---
        old_body = list_merge(SPLITBODY[:cluster_end + 1])
        # print(old_body)
        MSG_dict['body:'] = old_body

        # The new forwarded message body is everything *after* the cluster
        newest_body = list_merge(SPLITBODY[cluster_end + 1:])

        newest_dict = {  
            "from:": "Name",
            "to:": "Name",
            "cc:": "Name",
            "subject:": "some text",
            "date:": "some date",
            "body:": newest_body
        }


        # Recurse into the forwarded part
        MSG_dict['fwd_mess'] = recursive_body_split(newest_dict, depth + 1, max_depth)
        #Remove last empty dict
        last_dict = MSG_dict["fwd_mess"]
        if last_dict["body:"] == "":
            del MSG_dict['fwd_mess']
        return MSG_dict
    
    upper_dict['fwd_mess'] = recursive_body_split(upper_dict['fwd_mess'])
    return upper_dict



#The function below takes the nested dictionaries and redistributes the proper headers.


def subject_handler(text: str) -> str:
    new = text.casefold()
    v = SUBJECT_RE.search(new)
    return text[v.end():], text[:v.end() ]


def from_handler(text: str) -> str:
    new = text.casefold()
    #print(new)
    v = EMAIL_ADRESS_RE.search(new)
    return text[v.end():], text[:v.end() ]




def to_handler(text: str, ls):
    new = text.casefold()
    v = EMAIL_ADRESS_RE.search(new)
    if v is not None:
        ls.append(text[:v.end()])
        return to_handler(text[v.end():], ls)
    return text, ls




def last_line_handler(dc: dict, h : str) -> dict:
    boddie =  normalize(dc['body:']).strip()
    k = LABEL_TO_CANON[h]
    body = k + boddie
    if k.casefold() == "from:":
        #return dc
        dc["body:"], dc["from:"] = from_handler(body)
    if k.casefold() == "cc:" or k.casefold() == "to:":
        #return dc
        dc["body:"], dc[k.casefold()] = to_handler(body, ls = [])
    if k.casefold() == "subject:":
        #print(dc['body:'])
        dc["body:"], dc["subject:"] = subject_handler(body)

    return dc




def redistribute(dc: dict) -> dict:
    """
    Recursively splits and maps sections of an email-like dict using header keywords.
    Example header keys: "From:", "To:", "Subject:", etc.
    """

    body = normalize(dc['body:']).strip()

    header_key, new_body = extract_header_word(body)
    canon_key = LABEL_TO_CANON[header_key]

    def find_next(body: str) -> int | None:
        """Return index of next header_key line based on LABEL_TO_CANON keys."""
        splitted = body.casefold().splitlines()
        for i, line in enumerate(splitted):
            if any(line.startswith(prefix) for prefix in LABEL_TO_CANON.keys()):
                #print(f"found Header key {body.splitlines()[i]}")  #------> DEBUGGING
                return i
        return None
    
    #def last_header_handler(text: str):
    next_idx = find_next(new_body)

    # We split the new body and search for the next header keyword.
    splitted = new_body.splitlines()
    #print(splitted)
    dc["body:"] = "\n".join(splitted)
    #print(dc)
    if find_next(new_body) is not None:
        # Map current header to content before next header
        dc[canon_key] = "\n".join(splitted[:next_idx])
        bod = "\n".join(splitted[next_idx:])
        dc["body:"] = bod
        dc = redistribute(dc)
        return(dc)
    #print(header_key)
    dc = last_line_handler(dc, header_key)
    return dc


def loop_over_dicts(DC: dict):
    '''
    Loops recusrively over all nested dictionaries
    '''
    if 'fwd_mess' in DC.keys():
        DC["fwd_mess"] = redistribute(DC["fwd_mess"])
        dictionar = DC["fwd_mess"]
        dictionar = loop_over_dicts(dictionar)
    return DC


    

        

    




#----------------------------
#   Process Files in batch
#----------------------------

def process_json(path: Path):
    print(f"/n === Processing {path.name} =====  ")
    try:
        data = json.load(path.open("r", encoding="utf-8"))
    except Exception as e:
        print(f"⚠️  Invalid JSON for {path.name}: {e}")
        return
    if not isinstance(data, dict):
        print(f"file {path.name} is not a json-dict.")
        return
    updated = transform_headers(change_key_format(data))
    structured = first_body_split(updated)
    structured_distributed = loop_over_dicts(structured)
    out_path = OUTPUT_DIR_seathreader / path.name
    try:
        json.dump(structured_distributed, out_path.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"✅  Saved → {out_path}")
    except Exception as e:
        print(f"❌  Failed to write {out_path.name}: {e}")

def batch_process():
    files = sorted(INPUT_DIR_seathreader.glob("*.json"))
    if not files:
        print(f"no json files found in {INPUT_DIR_seathreader.name}")
        return
    print(f"Found {len(files)} files in {INPUT_DIR_seathreader}")
    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}]")
        process_json(f)

#----------------------------
#   MAIN
#----------------------------
if __name__ == "__main__":
    batch_process()
