from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from email.utils import parseaddr
from typing import Any


DEFAULT_AUTO_LABEL = "VIP"


def normalize_email(value: str) -> str:
    _name, addr = parseaddr(value or "")
    email = (addr or value or "").strip().lower()
    if "@" not in email or email.startswith("@") or email.endswith("@"):
        return ""
    return email


def ensure_vip_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vip_contacts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          display_name TEXT NOT NULL,
          email TEXT NOT NULL,
          domain TEXT,
          tier TEXT DEFAULT 'standard',
          active INTEGER DEFAULT 1,
          notes TEXT,
          always_notify INTEGER DEFAULT 1,
          digest_only INTEGER DEFAULT 0,
          auto_label TEXT DEFAULT 'VIP',
          never_archive INTEGER DEFAULT 1,
          sla_minutes INTEGER,
          priority_score INTEGER DEFAULT 0,
          created_at TEXT DEFAULT (datetime('now')),
          updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vip_contacts_email ON vip_contacts(email)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vip_contacts_domain ON vip_contacts(domain)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vip_match_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          message_uid INTEGER,
          message_id TEXT,
          thread_id TEXT,
          from_email TEXT NOT NULL,
          vip_contact_id INTEGER,
          match_type TEXT NOT NULL,
          actions_json TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vip_audit_created ON vip_match_audit(created_at DESC)")

    _ensure_message_columns(conn)
    conn.commit()


def _ensure_message_columns(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)").fetchall()}
    wanted = {
        "vip_contact_id": "INTEGER",
        "vip_match_type": "TEXT",
        "vip_actions_json": "TEXT",
        "vip_matched_at": "TEXT",
    }
    for name, col_type in wanted.items():
        if name not in cols:
            conn.execute(f"ALTER TABLE messages ADD COLUMN {name} {col_type}")


def vip_record_to_row(payload: dict[str, Any]) -> dict[str, Any]:
    email = normalize_email(payload.get("email", ""))
    if not email:
        raise ValueError("A valid email is required")

    domain = (payload.get("domain") or "").strip().lower()
    if domain.startswith("@"):
        domain = domain[1:]

    if domain and ("." not in domain or " " in domain):
        raise ValueError("domain must look like example.com")

    tier = (payload.get("tier") or "standard").strip() or "standard"
    auto_label = (payload.get("auto_label") or DEFAULT_AUTO_LABEL).strip() or DEFAULT_AUTO_LABEL

    sla_raw = payload.get("sla_minutes")
    sla_minutes = None
    if sla_raw not in (None, ""):
        try:
            sla_minutes = int(sla_raw)
            if sla_minutes < 0:
                raise ValueError
        except Exception:
            raise ValueError("sla_minutes must be a non-negative integer")

    priority_raw = payload.get("priority_score", 0)
    try:
        priority_score = int(priority_raw or 0)
    except Exception:
        raise ValueError("priority_score must be an integer")

    return {
        "display_name": (payload.get("display_name") or "").strip() or email,
        "email": email,
        "domain": domain or None,
        "tier": tier,
        "active": 1 if _truthy(payload.get("active", True)) else 0,
        "notes": (payload.get("notes") or "").strip(),
        "always_notify": 1 if _truthy(payload.get("always_notify", True)) else 0,
        "digest_only": 1 if _truthy(payload.get("digest_only", False)) else 0,
        "auto_label": auto_label,
        "never_archive": 1 if _truthy(payload.get("never_archive", True)) else 0,
        "sla_minutes": sla_minutes,
        "priority_score": priority_score,
    }


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def upsert_vip(conn: sqlite3.Connection, payload: dict[str, Any], vip_id: int | None = None) -> int:
    row = vip_record_to_row(payload)
    now = datetime.now().isoformat(timespec="seconds")

    if vip_id is None:
        try:
            cur = conn.execute(
                """
                INSERT INTO vip_contacts (
                  display_name, email, domain, tier, active, notes,
                  always_notify, digest_only, auto_label, never_archive,
                  sla_minutes, priority_score, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["display_name"], row["email"], row["domain"], row["tier"], row["active"], row["notes"],
                    row["always_notify"], row["digest_only"], row["auto_label"], row["never_archive"],
                    row["sla_minutes"], row["priority_score"], now, now,
                ),
            )
        except sqlite3.IntegrityError as e:
            raise ValueError(f"Could not save VIP: {e}")
        conn.commit()
        return int(cur.lastrowid)

    exists = conn.execute("SELECT id FROM vip_contacts WHERE id=?", (vip_id,)).fetchone()
    if not exists:
        raise ValueError("VIP not found")

    try:
        conn.execute(
            """
            UPDATE vip_contacts
            SET display_name=?, email=?, domain=?, tier=?, active=?, notes=?,
                always_notify=?, digest_only=?, auto_label=?, never_archive=?,
                sla_minutes=?, priority_score=?, updated_at=?
            WHERE id=?
            """,
            (
                row["display_name"], row["email"], row["domain"], row["tier"], row["active"], row["notes"],
                row["always_notify"], row["digest_only"], row["auto_label"], row["never_archive"],
                row["sla_minutes"], row["priority_score"], now, vip_id,
            ),
        )
    except sqlite3.IntegrityError as e:
        raise ValueError(f"Could not update VIP: {e}")
    conn.commit()
    return vip_id


def list_vips(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM vip_contacts ORDER BY active DESC, priority_score DESC, updated_at DESC, id DESC"
    ).fetchall()


def get_vip(conn: sqlite3.Connection, vip_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM vip_contacts WHERE id=?", (vip_id,)).fetchone()


def delete_vip(conn: sqlite3.Connection, vip_id: int) -> bool:
    cur = conn.execute("DELETE FROM vip_contacts WHERE id=?", (vip_id,))
    conn.commit()
    return cur.rowcount > 0


def match_vip(conn: sqlite3.Connection, sender_email_or_header: str) -> dict[str, Any] | None:
    from_email = normalize_email(sender_email_or_header)
    if not from_email:
        return None

    local, _, domain = from_email.partition("@")
    if not local or not domain:
        return None

    exact = conn.execute(
        """
        SELECT * FROM vip_contacts
        WHERE active=1 AND lower(email)=?
        ORDER BY priority_score DESC, id ASC
        LIMIT 1
        """,
        (from_email,),
    ).fetchone()

    if exact:
        return _build_match(exact, from_email, "exact")

    by_domain = conn.execute(
        """
        SELECT * FROM vip_contacts
        WHERE active=1 AND lower(domain)=?
        ORDER BY priority_score DESC, id ASC
        LIMIT 1
        """,
        (domain,),
    ).fetchone()
    if by_domain:
        return _build_match(by_domain, from_email, "domain")
    return None


def _build_match(vip: sqlite3.Row, from_email: str, match_type: str) -> dict[str, Any]:
    actions = {
        "always_notify": bool(vip["always_notify"]),
        "digest_only": bool(vip["digest_only"]),
        "auto_label": vip["auto_label"] or DEFAULT_AUTO_LABEL,
        "never_archive": bool(vip["never_archive"]),
        "sla_minutes": vip["sla_minutes"],
        "priority_score": vip["priority_score"],
        "tier": vip["tier"],
    }
    return {
        "vip_contact_id": vip["id"],
        "display_name": vip["display_name"],
        "from_email": from_email,
        "match_type": match_type,
        "actions": actions,
    }


def apply_vip_match_to_message(
    conn: sqlite3.Connection,
    *,
    uid: int,
    message_id: str,
    thread_id: str,
    sender_header: str,
) -> dict[str, Any] | None:
    match = match_vip(conn, sender_header)
    if not match:
        conn.execute(
            "UPDATE messages SET vip_contact_id=NULL, vip_match_type=NULL, vip_actions_json=NULL, vip_matched_at=NULL WHERE uid=?",
            (uid,),
        )
        conn.commit()
        return None

    actions_json = json.dumps(match["actions"], separators=(",", ":"))
    conn.execute(
        """
        UPDATE messages
        SET vip_contact_id=?, vip_match_type=?, vip_actions_json=?, vip_matched_at=datetime('now')
        WHERE uid=?
        """,
        (match["vip_contact_id"], match["match_type"], actions_json, uid),
    )

    conn.execute(
        """
        INSERT INTO vip_match_audit(message_uid, message_id, thread_id, from_email, vip_contact_id, match_type, actions_json)
        VALUES(?,?,?,?,?,?,?)
        """,
        (uid, message_id or "", thread_id or "", match["from_email"], match["vip_contact_id"], match["match_type"], actions_json),
    )
    conn.commit()
    return match


def recent_vip_audit(conn: sqlite3.Connection, limit: int = 50) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT a.*, v.display_name
        FROM vip_match_audit a
        LEFT JOIN vip_contacts v ON v.id = a.vip_contact_id
        ORDER BY a.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
