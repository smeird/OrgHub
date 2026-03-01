#!/usr/bin/env python3
"""Organization Hub + modules (Email Filing + Processes)."""

from __future__ import annotations
from typing import Optional

import json
import os
import sqlite3
import mimetypes
import subprocess
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from pydantic import BaseModel
from starlette.templating import Jinja2Templates

from vectorize import embed_texts

BASE_DIR = Path.home() / "Documents" / "Email-Attachments"
DB_PATH = BASE_DIR / "attachments.db"
AUTOMATION_BASE = Path.home() / "AutomationHub"
PROCESS_MGR = AUTOMATION_BASE / "bin" / "process_manager.py"

load_dotenv(BASE_DIR / ".env")

app = FastAPI(title="Dom Organization Hub")
templates = Jinja2Templates(directory=str(BASE_DIR / "web"))


class SearchResult(BaseModel):
    chunk_id: int
    attachment_id: int
    score: float
    snippet: str
    attachment_name: str
    category: Optional[str]
    tags: list[str]
    message_subject: str
    message_sender: str
    message_date: str
    path_raw: str


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def cosine_similarity(vec_a, vec_b):
    from math import sqrt

    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = sqrt(sum(a * a for a in vec_a))
    norm_b = sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def get_stats(conn: sqlite3.Connection) -> dict:
    attachments = conn.execute("SELECT COUNT(*) FROM attachments").fetchone()[0]
    chunks = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    latest = conn.execute("SELECT MAX(created_at) FROM attachments").fetchone()[0]
    storage = conn.execute("SELECT IFNULL(SUM(size_bytes), 0) FROM attachments").fetchone()[0]
    categories = conn.execute("SELECT category, COUNT(*) AS c FROM attachments GROUP BY category ORDER BY c DESC").fetchall()
    senders = conn.execute("SELECT sender, COUNT(*) AS c FROM messages GROUP BY sender ORDER BY c DESC LIMIT 8").fetchall()
    tags = conn.execute("SELECT tag, COUNT(*) AS c FROM tags_view ORDER BY c DESC LIMIT 10").fetchall()
    return {
        "attachments": attachments,
        "chunks": chunks,
        "latest": latest,
        "storage": storage,
        "categories": categories,
        "senders": senders,
        "tags": tags,
    }


def search(conn, query_embedding, limit=10, category: Optional[str] = None, sender: Optional[str] = None, tag: Optional[str] = None):
    base = (
        "SELECT chunks.id, chunks.text, chunks.attachment_id, chunks.embedding, attachments.id AS attachment_id, "
        "attachments.normalized_name, attachments.path_raw, attachments.category, attachments.tags, messages.subject, messages.sender, messages.date "
        "FROM chunks "
        "JOIN attachments ON chunks.attachment_id=attachments.id "
        "JOIN messages ON attachments.message_uid=messages.uid"
    )
    conditions = []
    params = []
    if category:
        conditions.append("attachments.category = ?")
        params.append(category)
    if sender:
        conditions.append("messages.sender = ?")
        params.append(sender)
    if tag:
        conditions.append("instr(',' || attachments.tags || ',', ',' || ? || ',') > 0")
        params.append(tag)
    if conditions:
        base += " WHERE " + " AND ".join(conditions)
    rows = conn.execute(base, params).fetchall()
    results = []
    for row in rows:
        embedding = json.loads(row["embedding"]) if row["embedding"] else []
        score = cosine_similarity(query_embedding, embedding)
        results.append(
            SearchResult(
                chunk_id=row["id"],
                attachment_id=row["attachment_id"],
                score=score,
                snippet=row["text"][:240],
                attachment_name=row["normalized_name"],
                category=row["category"],
                tags=[tag for tag in (row["tags"] or "").split(",") if tag],
                message_subject=row["subject"],
                message_sender=row["sender"],
                message_date=row["date"],
                path_raw=row["path_raw"],
            )
        )
    results.sort(key=lambda r: r.score, reverse=True)
    return results[:limit]


def _launchctl_process_info(slug: str) -> dict:
    label = f"gui/501/com.dom.process.{slug}"
    res = subprocess.run(["launchctl", "print", label], capture_output=True, text=True)
    text = (res.stdout or "") + (res.stderr or "")

    info = {"last_exit_code": None, "last_result": "Unknown"}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("last exit code ="):
            try:
                code = int(line.split("=", 1)[1].strip())
                info["last_exit_code"] = code
                info["last_result"] = "Success" if code == 0 else f"Failed ({code})"
            except Exception:
                pass
            break
    return info


def _last_run_time(slug: str) -> str:
    out_log = AUTOMATION_BASE / "logs" / f"{slug}.out.log"
    err_log = AUTOMATION_BASE / "logs" / f"{slug}.err.log"

    candidates = []
    for p in (out_log, err_log):
        if p.exists():
            try:
                candidates.append(p.stat().st_mtime)
            except Exception:
                pass

    if not candidates:
        return "Never"

    return datetime.fromtimestamp(max(candidates)).strftime("%Y-%m-%d %H:%M")


def list_processes() -> list[dict]:
    if not PROCESS_MGR.exists():
        return []
    res = subprocess.run([str(PROCESS_MGR), "list"], capture_output=True, text=True)
    if res.returncode != 0:
        return []
    try:
        items = json.loads(res.stdout or "[]")
    except Exception:
        return []

    for item in items:
        slug = item.get("slug", "")
        extra = _launchctl_process_info(slug) if slug else {"last_exit_code": None, "last_result": "Unknown"}
        item["last_exit_code"] = extra["last_exit_code"]
        item["last_result"] = extra["last_result"]
        item["last_ran_at"] = _last_run_time(slug) if slug else "Never"

    return items


@app.get("/", response_class=HTMLResponse)
async def hub(request: Request):
    conn = connect_db()
    stats = get_stats(conn)
    process_count = len(list_processes())
    modules = [
        {"name": "Email Filing", "status": "Live", "desc": "Search and triage email attachments.", "href": "/tools/email-filing"},
        {"name": "Processes", "status": "Live", "desc": f"Manage automations ({process_count} configured).", "href": "/tools/processes"},
        {"name": "Tasks", "status": "Planned", "desc": "Upcoming task workflows.", "href": "#"},
        {"name": "Calendar", "status": "Planned", "desc": "Upcoming schedule dashboard.", "href": "#"},
    ]
    return templates.TemplateResponse("hub.html", {"request": request, "stats": stats, "modules": modules, "active_nav": "hub"})


@app.get("/tools/processes", response_class=HTMLResponse)
async def processes(request: Request, msg: str = ""):
    items = list_processes()
    return templates.TemplateResponse("processes.html", {"request": request, "items": items, "msg": msg, "active_nav": "processes"})


@app.get("/tools/processes/create")
async def process_create(name: str, interval: int = 300, description: str = ""):
    if not PROCESS_MGR.exists():
        return RedirectResponse(url="/tools/processes?msg=Process+manager+missing", status_code=302)
    subprocess.run([str(PROCESS_MGR), "scaffold", name, "--interval", str(interval), "--description", description], capture_output=True, text=True)
    subprocess.run([str(PROCESS_MGR), "enable", name], capture_output=True, text=True)
    return RedirectResponse(url="/tools/processes?msg=Process+created", status_code=302)


@app.get("/tools/processes/enable")
async def process_enable(name: str):
    subprocess.run([str(PROCESS_MGR), "enable", name], capture_output=True, text=True)
    return RedirectResponse(url="/tools/processes?msg=Process+enabled", status_code=302)


@app.get("/tools/processes/disable")
async def process_disable(name: str):
    subprocess.run([str(PROCESS_MGR), "disable", name], capture_output=True, text=True)
    return RedirectResponse(url="/tools/processes?msg=Process+disabled", status_code=302)


@app.get("/tools/email-filing", response_class=HTMLResponse)
@app.get("/email-filing", response_class=HTMLResponse)
async def email_filing(
    request: Request,
    q: str = Query(""),
    category: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    tag: Optional[str] = Query(None),
):
    conn = connect_db()
    stats = get_stats(conn)
    results = []
    query = q.strip()
    selected_category = category or ""
    selected_sender = sender or ""
    selected_tag = tag or ""
    if query:
        embedding = embed_texts([query])[0]
        results = search(conn, embedding, category=selected_category or None, sender=selected_sender or None, tag=selected_tag or None)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "results": results,
            "query": query,
            "stats": stats,
            "category": selected_category,
            "sender": selected_sender,
            "tag": selected_tag,
            "active_nav": "email",
        },
    )


@app.get("/preview/{attachment_id}")
async def preview(attachment_id: int):
    conn = connect_db()
    row = conn.execute("SELECT path_raw FROM attachments WHERE id=?", (attachment_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Attachment not found")
    file_path = row["path_raw"]
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File missing")
    mime, _ = mimetypes.guess_type(file_path)
    return FileResponse(file_path, media_type=mime or "application/octet-stream")


@app.get("/attachment/{attachment_id}", response_class=HTMLResponse)
async def attachment_detail(request: Request, attachment_id: int):
    conn = connect_db()
    row = conn.execute(
        "SELECT attachments.*, messages.subject, messages.sender, messages.date FROM attachments JOIN messages ON attachments.message_uid = messages.uid WHERE attachments.id=?",
        (attachment_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Attachment not found")
    text_path = row["path_text"]
    text_content = ""
    if text_path and os.path.exists(text_path):
        text_content = Path(text_path).read_text(errors="ignore")
    return templates.TemplateResponse("detail.html", {"request": request, "attachment": row, "text_content": text_content, "active_nav": "email"})
