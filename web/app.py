#!/usr/bin/env python3
"""Organization Hub + modules (Email Filing + Processes + Calendar)."""

from __future__ import annotations
from typing import Optional

import json
import os
import sqlite3
import mimetypes
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from pydantic import BaseModel
from starlette.templating import Jinja2Templates

from vectorize import embed_texts

BASE_DIR = Path(os.getenv("EMAIL_ATTACHMENTS_BASE", str(Path.home() / "AutomationHub" / "email-filing")))
DB_PATH = BASE_DIR / "attachments.db"
AUTOMATION_BASE = Path.home() / "AutomationHub"
PROCESS_MGR = AUTOMATION_BASE / "bin" / "process_manager.py"
CALENDAR_NAME = "BundyFamily"

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


def run_osascript(script: str, args: list[str] | None = None) -> subprocess.CompletedProcess:
    cmd = ["osascript", "-"]
    if args:
        cmd.extend(args)
    return subprocess.run(cmd, input=script, text=True, capture_output=True)


def calendar_window_days(window: str) -> int:
    return {"today": 1, "week": 7, "14d": 14}.get(window, 14)


def list_calendar_events(limit: int = 30, window: str = "14d") -> list[dict]:
    days = calendar_window_days(window)
    script = r'''
on run argv
  set calName to item 1 of argv
  set maxCount to (item 2 of argv) as integer
  set dayCount to (item 3 of argv) as integer
  tell application "Calendar"
    if not (exists calendar calName) then
      return "__NO_CALENDAR__"
    end if
    set fromDate to (current date)
    set toDate to fromDate + (dayCount * days)
    set evs to (every event of calendar calName whose start date ≥ fromDate and start date ≤ toDate)
    set sortedEvs to my sortEvents(evs)
    set outLines to ""
    set i to 0
    repeat with ev in sortedEvs
      set i to i + 1
      if i > maxCount then exit repeat
      set uidText to uid of ev
      set t to summary of ev
      set sd to (start date of ev) as string
      set ed to (end date of ev) as string
      set l to location of ev
      if l is missing value then set l to ""
      set d to description of ev
      if d is missing value then set d to ""
      set outLines to outLines & uidText & "\t" & t & "\t" & sd & "\t" & ed & "\t" & l & "\t" & d & "\n"
    end repeat
    return outLines
  end tell
end run

on sortEvents(evs)
  tell application "Calendar"
    set sortedEvs to evs
    set n to count of sortedEvs
    repeat with i from 1 to n
      repeat with j from i + 1 to n
        if (start date of item j of sortedEvs) < (start date of item i of sortedEvs) then
          set tmp to item i of sortedEvs
          set item i of sortedEvs to item j of sortedEvs
          set item j of sortedEvs to tmp
        end if
      end repeat
    end repeat
    return sortedEvs
  end tell
end sortEvents
'''
    res = run_osascript(script, [CALENDAR_NAME, str(limit), str(days)])
    if res.returncode != 0:
        return []
    out = (res.stdout or "").strip()
    if out == "__NO_CALENDAR__" or not out:
        return []
    events = []
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) >= 5:
            events.append(
                {
                    "uid": parts[0],
                    "title": parts[1],
                    "start": parts[2],
                    "end": parts[3],
                    "location": parts[4],
                    "notes": parts[5] if len(parts) > 5 else "",
                }
            )
    return events


def create_or_update_calendar_event(title: str, start_iso: str, end_iso: str, notes: str = "", uid: str = "") -> tuple[bool, str]:
    script = r'''
on parseISO(isoText)
  set oldTIDs to AppleScript's text item delimiters
  set AppleScript's text item delimiters to {"-", "T", ":"}
  set parts to text items of isoText
  set AppleScript's text item delimiters to oldTIDs

  set yy to item 1 of parts as integer
  set mm to item 2 of parts as integer
  set dd to item 3 of parts as integer
  set hh to item 4 of parts as integer
  set mi to item 5 of parts as integer

  set d to current date
  set year of d to yy
  set month of d to mm
  set day of d to dd
  set hours of d to hh
  set minutes of d to mi
  set seconds of d to 0
  return d
end parseISO

on run argv
  set calName to item 1 of argv
  set evTitle to item 2 of argv
  set startISO to item 3 of argv
  set endISO to item 4 of argv
  set evNotes to item 5 of argv
  set evUid to item 6 of argv

  set sDate to my parseISO(startISO)
  set eDate to my parseISO(endISO)

  tell application "Calendar"
    if not (exists calendar calName) then
      return "ERR: Calendar not found"
    end if

    tell calendar calName
      if evUid is not "" then
        set matches to (every event whose uid is evUid)
        if (count of matches) > 0 then
          set tgt to item 1 of matches
          set summary of tgt to evTitle
          set start date of tgt to sDate
          set end date of tgt to eDate
          set description of tgt to evNotes
          return "UPDATED"
        end if
      end if

      set newEvent to make new event with properties {summary:evTitle, start date:sDate, end date:eDate, description:evNotes}
      tell newEvent
        make new display alarm at end with properties {trigger interval:-60}
      end tell
    end tell
  end tell
  return "CREATED"
end run
'''
    res = run_osascript(script, [CALENDAR_NAME, title, start_iso, end_iso, notes, uid])
    output = (res.stdout or "").strip() or (res.stderr or "").strip()
    return (res.returncode == 0 and output in {"CREATED", "UPDATED"}, output)


def delete_calendar_event(uid: str) -> tuple[bool, str]:
    script = r'''
on run argv
  set calName to item 1 of argv
  set evUid to item 2 of argv
  tell application "Calendar"
    if not (exists calendar calName) then
      return "ERR: Calendar not found"
    end if
    tell calendar calName
      set matches to (every event whose uid is evUid)
      if (count of matches) = 0 then
        return "NOT_FOUND"
      end if
      delete item 1 of matches
      return "DELETED"
    end tell
  end tell
end run
'''
    res = run_osascript(script, [CALENDAR_NAME, uid])
    output = (res.stdout or "").strip() or (res.stderr or "").strip()
    return (res.returncode == 0 and output == "DELETED", output)


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


def _format_ago(ts: float) -> str:
    now = datetime.now().timestamp()
    diff = max(0, int(now - ts))

    if diff < 60:
        return "just now"
    if diff < 3600:
        mins = diff // 60
        return f"{mins}m ago"
    if diff < 86400:
        hrs = diff // 3600
        return f"{hrs}h ago"
    days = diff // 86400
    return f"{days}d ago"


def _last_run_time(slug: str) -> tuple[str, str]:
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
        return "Never", "never"

    ts = max(candidates)
    exact = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    ago = _format_ago(ts)
    return exact, ago


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
        exact, ago = _last_run_time(slug) if slug else ("Never", "never")
        item["last_ran_at"] = exact
        item["last_ran_ago"] = ago

    return items


@app.get("/", response_class=HTMLResponse)
async def hub(request: Request):
    conn = connect_db()
    stats = get_stats(conn)
    process_count = len(list_processes())
    modules = [
        {"name": "Email Filing", "status": "Live", "desc": "Search and triage email attachments.", "href": "/tools/email-filing"},
        {"name": "Processes", "status": "Live", "desc": f"Manage automations ({process_count} configured).", "href": "/tools/processes"},
        {"name": "Calendar", "status": "Live", "desc": "BundyFamily calendar (RW) with 1-hour reminders.", "href": "/tools/calendar"},
        {"name": "Tasks", "status": "Planned", "desc": "Upcoming task workflows.", "href": "#"},
    ]
    runtime_path = str(BASE_DIR)
    canonical_path = str(Path.home() / "AutomationHub" / "email-filing")
    path_status = "Running from canonical path" if runtime_path == canonical_path else f"Running from non-canonical path: {runtime_path}"
    return templates.TemplateResponse("hub.html", {"request": request, "stats": stats, "modules": modules, "active_nav": "hub", "path_status": path_status, "runtime_path": runtime_path})


@app.get("/tools/calendar", response_class=HTMLResponse)
async def calendar_view(
    request: Request,
    msg: str = "",
    window: str = "14d",
    edit_uid: str = "",
    title: str = "",
    start: str = "",
    end: str = "",
    notes: str = "",
):
    events = list_calendar_events(limit=40, window=window)
    edit_event = next((e for e in events if e["uid"] == edit_uid), None)

    now_local = datetime.now().strftime("%Y-%m-%dT%H:%M")
    default_end = (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")

    form_title = title or (edit_event["title"] if edit_event else "")
    form_start = start or now_local
    form_end = end or default_end
    form_notes = notes or (edit_event["notes"] if edit_event else "")

    return templates.TemplateResponse(
        "calendar.html",
        {
            "request": request,
            "events": events,
            "calendar_name": CALENDAR_NAME,
            "msg": msg,
            "window": window,
            "active_nav": "calendar",
            "edit_uid": edit_uid,
            "form_title": form_title,
            "form_start": form_start,
            "form_end": form_end,
            "form_notes": form_notes,
        },
    )


@app.get("/tools/calendar/save")
async def calendar_save(title: str, start: str, end: str, notes: str = "", uid: str = ""):
    ok, out = create_or_update_calendar_event(title, start, end, notes, uid)
    if ok:
        msg = "Event+updated" if out == "UPDATED" else "Event+created"
        return RedirectResponse(url=f"/tools/calendar?msg={msg}", status_code=302)
    return RedirectResponse(url=f"/tools/calendar?msg=Save+failed:+{out[:120]}", status_code=302)


@app.get("/tools/calendar/delete")
async def calendar_delete(uid: str):
    ok, out = delete_calendar_event(uid)
    if ok:
        return RedirectResponse(url="/tools/calendar?msg=Event+deleted", status_code=302)
    return RedirectResponse(url=f"/tools/calendar?msg=Delete+failed:+{out[:120]}", status_code=302)


@app.get("/tools/calendar/from-attachment/{attachment_id}")
async def calendar_from_attachment(attachment_id: int):
    conn = connect_db()
    row = conn.execute(
        "SELECT messages.subject, messages.date, attachments.normalized_name FROM attachments JOIN messages ON attachments.message_uid=messages.uid WHERE attachments.id=?",
        (attachment_id,),
    ).fetchone()
    if not row:
        return RedirectResponse(url="/tools/calendar?msg=Attachment+not+found", status_code=302)

    title = f"Review: {row['subject'] or row['normalized_name']}"
    start = datetime.now().replace(second=0, microsecond=0)
    end = start + timedelta(hours=1)
    params = {
        "title": title,
        "start": start.strftime("%Y-%m-%dT%H:%M"),
        "end": end.strftime("%Y-%m-%dT%H:%M"),
        "notes": f"Linked attachment: {row['normalized_name']}",
    }
    from urllib.parse import urlencode
    return RedirectResponse(url=f"/tools/calendar?{urlencode(params)}", status_code=302)


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
