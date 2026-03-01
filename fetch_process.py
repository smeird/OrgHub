#!/usr/bin/env python3
"""Ingest iCloud mail attachments into SQLite + append short sender summaries to Obsidian."""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import sqlite3
import textwrap
from datetime import datetime
from email.utils import parseaddr
from typing import Optional

from dotenv import load_dotenv
from imap_tools import AND, MailBox

from extract_text import extract_text_from_file
from vectorize import chunk_and_embed

BASE_DIR = pathlib.Path(os.getenv("EMAIL_ATTACHMENTS_BASE", str(pathlib.Path.home() / "AutomationHub" / "email-filing")))
RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed"
LOG_FILE = BASE_DIR / "logs" / "ingest.log"
DB_PATH = BASE_DIR / "attachments.db"
OBSIDIAN_CONFIG_PATH = pathlib.Path.home() / "Library/Application Support/obsidian/obsidian.json"

START_YEAR = int(os.getenv("INGEST_START_YEAR", "2025"))
SUMMARY_NOTES_FOLDER = os.getenv("OBSIDIAN_SUMMARY_FOLDER", "Email Summaries")

CATEGORY_RULES = {
    "Finance": ["invoice", "statement", "receipt", "tax", "bank"],
    "Legal": ["contract", "agreement", "legal", "equity", "solicitor"],
    "Family": ["school", "kids", "family", "birthday"],
    "Travel": ["booking", "travel", "flight", "hotel"],
}

TAG_RULES = {
    "Invoice": ["invoice", "receipt", "gst"],
    "Statement": ["statement", "balance"],
    "Contract": ["contract", "agreement", "nda"],
    "ID": ["passport", "license", "id"],
}


def log(message: str) -> None:
    timestamp = datetime.now().isoformat(timespec="seconds")
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a") as fh:
        fh.write(f"[{timestamp}] {message}\n")
    print(message)


def detect_tags(subject: str, sender: str, filename: str) -> list[str]:
    haystack = f"{subject} {sender} {filename}".lower()
    tags = []
    for tag, keywords in TAG_RULES.items():
        if any(kw in haystack for kw in keywords):
            tags.append(tag)
    return tags


def classify_attachment(subject: str, sender: str, filename: str) -> str:
    haystack = f"{subject} {sender} {filename}".lower()
    for category, keywords in CATEGORY_RULES.items():
        if any(kw in haystack for kw in keywords):
            return category
    return "Unsorted"


def normalize_filename(msg_date: datetime, sender: str, original: str) -> str:
    safe_sender = sender.replace("@", "_").replace(" ", "_")[:40]
    stem, ext = os.path.splitext(original)
    safe_stem = stem.replace(" ", "_")[:40] or "attachment"
    date_str = msg_date.strftime("%Y%m%d")
    return f"{date_str}_{safe_sender}_{safe_stem}{ext.lower()}"


def checksum(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_last_uid(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT value FROM settings WHERE key='last_uid'").fetchone()
    return int(row[0]) if row and row[0] else 0


def set_last_uid(conn: sqlite3.Connection, uid: int) -> None:
    conn.execute(
        "INSERT INTO settings(key,value) VALUES('last_uid',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(uid),),
    )
    conn.commit()


def save_message(conn: sqlite3.Connection, uid: int, msg) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO messages(uid, message_id, subject, sender, recipient, date, snippet, fetched_at)
        VALUES(?,?,?,?,?,?,?,datetime('now'))
        """,
        (
            uid,
            getattr(msg, "message_id", "") or "",
            msg.subject or "",
            msg.from_ or "",
            ", ".join(msg.to or []),
            msg.date_str or "",
            textwrap.shorten((msg.text or msg.html or ""), width=240),
        ),
    )


def resolve_obsidian_vault_path() -> Optional[pathlib.Path]:
    env_path = os.getenv("OBSIDIAN_VAULT_PATH")
    if env_path:
        p = pathlib.Path(env_path).expanduser()
        return p if p.exists() else None

    if not OBSIDIAN_CONFIG_PATH.exists():
        return None

    try:
        config = json.loads(OBSIDIAN_CONFIG_PATH.read_text())
    except Exception:
        return None

    vaults = config.get("vaults", {})
    open_vault = next((v for v in vaults.values() if v.get("open")), None)
    chosen = open_vault or (next(iter(vaults.values())) if vaults else None)
    if not chosen:
        return None

    path = pathlib.Path(chosen.get("path", "")).expanduser()
    return path if path.exists() else None


def slug_sender(sender: str) -> str:
    name, email = parseaddr(sender or "")
    base = name.strip() or email.split("@")[0] or "Unknown Sender"
    base = base.replace("'", "")
    base = re.sub(r"[^A-Za-z0-9 _-]+", "", base).strip()
    return base.title() or "Unknown Sender"


def append_summary_line(
    vault_path: Optional[pathlib.Path],
    msg_date: datetime,
    sender: str,
    subject: str,
    original_name: str,
    category: str,
    tags: list[str],
) -> None:
    if not vault_path:
        return

    note_dir = vault_path / SUMMARY_NOTES_FOLDER
    note_dir.mkdir(parents=True, exist_ok=True)

    sender_note_name = slug_sender(sender)
    note_path = note_dir / f"{sender_note_name}.md"

    date_text = msg_date.strftime("%Y-%m-%d")
    compact_subject = (subject or "(no subject)").replace("\n", " ").strip()
    tags_text = f" | tags: {', '.join(tags)}" if tags else ""
    line = f"- {date_text} | {compact_subject} | file: {original_name} | category: {category}{tags_text}"

    if note_path.exists():
        existing = note_path.read_text(errors="ignore")
        if line in existing:
            return
        with note_path.open("a") as fh:
            fh.write(line + "\n")
    else:
        header = f"# {sender_note_name}\n\n"
        note_path.write_text(header + line + "\n")


def process_attachment(conn: sqlite3.Connection, vault_path: Optional[pathlib.Path], uid: int, msg, att) -> bool:
    msg_date = msg.date or datetime.now()
    sender = msg.from_ or "unknown"
    subject = msg.subject or ""
    original_name = att.filename or "attachment.bin"
    normalized = normalize_filename(msg_date, sender, original_name)

    existing = conn.execute(
        "SELECT id FROM attachments WHERE message_uid=? AND original_name=?",
        (uid, original_name),
    ).fetchone()
    if existing:
        return False

    year_dir = RAW_DIR / str(msg_date.year) / str(uid)
    year_dir.mkdir(parents=True, exist_ok=True)
    raw_path = year_dir / normalized
    raw_path.write_bytes(att.payload)
    digest = checksum(raw_path)

    category = classify_attachment(subject, sender, normalized)
    tags = detect_tags(subject, sender, normalized)

    conn.execute(
        """
        INSERT INTO attachments
        (message_uid, original_name, normalized_name, mime_type, size_bytes, checksum, path_raw, status, category, tags)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            uid,
            original_name,
            normalized,
            att.content_type,
            len(att.payload),
            digest,
            str(raw_path),
            "saved",
            category,
            ",".join(tags),
        ),
    )
    attachment_id = conn.execute(
        "SELECT id FROM attachments WHERE message_uid=? AND original_name=?",
        (uid, original_name),
    ).fetchone()[0]

    text_path = PROCESSED_DIR / str(msg_date.year) / str(uid)
    text_path.mkdir(parents=True, exist_ok=True)
    text_file = text_path / f"{normalized}.txt"
    extracted_text = extract_text_from_file(raw_path, att.content_type)
    text_file.write_text(extracted_text)
    conn.execute(
        "UPDATE attachments SET path_text=?, status=?, updated_at=datetime('now') WHERE id=?",
        (str(text_file), "extracted", attachment_id),
    )

    chunk_and_embed(conn, attachment_id, extracted_text)
    append_summary_line(vault_path, msg_date, sender, subject, original_name, category, tags)
    return True


def main() -> None:
    load_dotenv(BASE_DIR / ".env")
    email_addr = os.getenv("ICLOUD_EMAIL")
    app_password = os.getenv("ICLOUD_APP_PASSWORD")
    host = os.getenv("IMAP_HOST", "imap.mail.me.com")

    if not email_addr or not app_password:
        raise RuntimeError("ICLOUD_EMAIL and ICLOUD_APP_PASSWORD must be set in .env")

    vault_path = resolve_obsidian_vault_path()
    if vault_path:
        log(f"Obsidian summaries enabled: {vault_path}")
    else:
        log("Obsidian vault not found; summary notes disabled for this run")

    conn = connect_db()
    last_uid = get_last_uid(conn)
    log(f"Last UID processed: {last_uid}")

    start_date = datetime(START_YEAR, 1, 1).date()
    query = AND(date_gte=start_date)
    processed = 0

    with MailBox(host).login(email_addr, app_password, initial_folder="INBOX") as mailbox:
        for msg in mailbox.fetch(query, mark_seen=False, bulk=True, limit=500, reverse=False):
            uid = int(msg.uid)
            if uid <= last_uid:
                continue

            save_message(conn, uid, msg)
            for att in msg.attachments:
                if process_attachment(conn, vault_path, uid, msg, att):
                    processed += 1

            set_last_uid(conn, uid)

    log(f"Processed {processed} attachments")


if __name__ == "__main__":
    main()
