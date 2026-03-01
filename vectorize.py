#!/usr/bin/env python3
"""Chunk text and store embeddings in SQLite."""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Iterable

import tiktoken
from dotenv import load_dotenv
from openai import OpenAI

BASE_DIR = os.path.expanduser("~/Documents/Email-Attachments")
load_dotenv(os.path.join(BASE_DIR, ".env"))
MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
encoding = tiktoken.get_encoding("cl100k_base")


def chunk_text(text: str, chunk_tokens: int = 500, overlap: int = 50) -> list[str]:
    tokens = encoding.encode(text)
    chunks = []
    start = 0
    while start < len(tokens):
        end = min(len(tokens), start + chunk_tokens)
        chunk = tokens[start:end]
        chunks.append(encoding.decode(chunk))
        if end == len(tokens):
            break
        start = max(0, end - overlap)
    return chunks


def embed_texts(texts: Iterable[str]) -> list[list[float]]:
    texts_list = list(texts)
    if not texts_list:
        return []
    resp = client.embeddings.create(model=MODEL, input=texts_list)
    return [d.embedding for d in resp.data]


def chunk_and_embed(conn: sqlite3.Connection, attachment_id: int, text: str) -> None:
    text = text.strip()
    if not text:
        return
    chunks = chunk_text(text)
    embeddings = embed_texts(chunks)
    conn.executemany(
        "INSERT INTO chunks(attachment_id, chunk_index, text, embedding) VALUES(?,?,?,?)",
        [
            (
                attachment_id,
                idx,
                chunk,
                json.dumps(vec),
            )
            for idx, (chunk, vec) in enumerate(zip(chunks, embeddings))
        ],
    )
    conn.commit()
