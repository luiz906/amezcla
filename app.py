import hashlib
import hmac
import json
import os
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from anthropic import Anthropic
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response

# ---------------------------------------------------------------------------
# Config (all via environment variables)
# ---------------------------------------------------------------------------
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = "13ccc694-e848-80ff-8543-fb2b57e1981a"
LMTZ_PAGE_ID = "17ecc694-e848-808f-84fd-d2f069a86ec9"

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

BLOTATO_API_KEY = os.environ["BLOTATO_API_KEY"]
BLOTATO_LINKEDIN_ACCOUNT_ID = os.environ["BLOTATO_LINKEDIN_ACCOUNT_ID"]

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET", "")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")
BRAND_KNOWLEDGE = os.environ.get("BRAND_KNOWLEDGE", "")

SCHEDULE_HOUR = int(os.environ.get("SCHEDULE_HOUR", "9"))
SCHEDULE_MINUTE = int(os.environ.get("SCHEDULE_MINUTE", "30"))

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ---------------------------------------------------------------------------
# Database (SQLite — persisted via Render disk)
# ---------------------------------------------------------------------------
DB_PATH = os.environ.get("DB_PATH", "reviews.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_reviews (
                id              TEXT PRIMARY KEY,
                notion_page_id  TEXT NOT NULL,
                post_name       TEXT,
                post_content    TEXT,
                status          TEXT DEFAULT 'pending',
                created_at      TEXT,
                slack_ts        TEXT,
                slack_channel   TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # Migrate existing tables that lack the new Slack columns
        for col in ("slack_ts", "slack_channel"):
            try:
                conn.execute(f"ALTER TABLE pending_reviews ADD COLUMN {col} TEXT")
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Notion helpers
# ---------------------------------------------------------------------------
async def find_notion_page() -> dict | None:
    """Return the first Not Started / LMTZ page, or None."""
    db_id = get_kv("notion_db_id", NOTION_DB_ID)
    lmtz_id = get_kv("lmtz_page_id", LMTZ_PAGE_ID)
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    body = {
        "filter": {
            "and": [
                {"property": "Status", "status": {"equals": "Not Started"}},
                {"property": "ClientsOS", "relation": {"contains": lmtz_id}},
            ]
        },
        "page_size": 1,
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json=body, headers=NOTION_HEADERS)
        r.raise_for_status()
    results = r.json().get("results", [])
    return results[0] if results else None


def _extract_page_name(page: dict) -> str:
    props = page.get("properties", {})
    for key in ("Post Name", "Name", "Title"):
        prop = props.get(key)
        if prop:
            titles = prop.get("title", [])
            return "".join(t.get("plain_text", "") for t in titles)
    return "(untitled)"


async def get_page_blocks_as_text(page_id: str) -> str:
    """Fetch page body blocks and return as plain text."""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=NOTION_HEADERS)
        r.raise_for_status()
    lines = []
    for block in r.json().get("results", []):
        btype = block.get("type", "")
        rich = block.get(btype, {}).get("rich_text", [])
        text = "".join(rt.get("plain_text", "") for rt in rich)
        if text:
            lines.append(text)
    return "\n".join(lines)


async def get_page_properties_as_text(page: dict) -> str:
    """Convert Notion page properties to a readable summary for Claude."""
    props = page.get("properties", {})
    lines = []
    for name, prop in props.items():
        ptype = prop.get("type")
        value = ""
        if ptype == "title":
            value = "".join(t.get("plain_text", "") for t in prop.get("title", []))
        elif ptype == "rich_text":
            value = "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))
        elif ptype == "select":
            sel = prop.get("select")
            value = sel.get("name", "") if sel else ""
        elif ptype == "status":
            s = prop.get("status")
            value = s.get("name", "") if s else ""
        elif ptype == "multi_select":
            value = ", ".join(o.get("name", "") for o in prop.get("multi_select", []))
        elif ptype == "date":
            d = prop.get("date")
            value = d.get("start", "") if d else ""
        elif ptype == "url":
            value = prop.get("url", "") or ""
        elif ptype == "number":
            value = str(prop.get("number", ""))
        if value:
            lines.append(f"{name}: {value}")
    return "\n".join(lines)


async def append_to_notion_page(page_id: str, content: str):
    """Append the generated draft to the Notion page."""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    # Split into chunks ≤ 2000 chars (Notion rich_text limit per block)
    chunks = [content[i:i + 2000] for i in range(0, len(content), 2000)]
    blocks = [
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": chunk}}]
            },
        }
        for chunk in chunks
    ]
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.patch(url, json={"children": blocks}, headers=NOTION_HEADERS)
        r.raise_for_status()


async def mark_notion_page_posted(page_id: str):
    """Update Status → Posted 🎉, set Posting Date and Type."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    now = datetime.now(timezone.utc).isoformat()
    body = {
        "properties": {
            "Status": {"status": {"name": "Posted 🎉"}},
            "Posting Date": {"date": {"start": now}},
            "Type": {"select": {"name": "Written Post"}},
        }
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.patch(url, json=body, headers=NOTION_HEADERS)
        r.raise_for_status()


# ---------------------------------------------------------------------------
# Claude
# ---------------------------------------------------------------------------
LINKEDIN_PROMPT = """\
You are a brand designer. Speak as an experienced graphic designer in simple language.

Write a short-form post based on the {post_name}.
Strong Hook – Make a bold polarizing statement that educates or entertains. It should instantly catch attention.
Rehook – Add a single line that builds curiosity or tension.
Main Content –
\t•\tWrite with natural rhythm and pacing.
\t•\tMix sentence lengths.
\t•\tTreat every line like it could stand alone.
\t•\tUse short lists or separators for clarity. Add bullet points if needed.
\t•\tKeep tone human and confident.
\t•\tAdd exactly 2 emojis for emphasis or visual pause.
Power Statement – End with a clear, definitive thought that feels like truth.
Call to Action – Finish with a short question or prompt that invites reflection or response.

Add exactly 3 hashtags inline at the end: one topic-specific, one target-audience-specific, one general. No labels, no bullet points — just the hashtags.
Output only the copy. No section titles, no labels like "Rehook:" "Title:", no formatting notes. DO NOT ADD "POWER STATEMENT:" Don't add #underdogs, replace with #smallbiz.

---
Page properties:
{properties}

Page content:
{content}\
"""


def get_kv(key: str, default: str = "") -> str:
    with get_db() as conn:
        row = conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_kv(key: str, value: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


def get_mock_mode() -> dict:
    with get_db() as conn:
        row = conn.execute("SELECT value FROM kv WHERE key='mock_mode'").fetchone()
    if not row:
        return {"enabled": False, "text": ""}
    try:
        return json.loads(row["value"])
    except Exception:
        return {"enabled": False, "text": ""}


def generate_linkedin_post(post_name: str, properties: str, content: str, mock_text: str = "") -> str:
    if mock_text:
        return mock_text
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    system = "You are a social media content expert and brand designer for Amezcla."
    bk = get_kv("brand_knowledge", BRAND_KNOWLEDGE)
    if bk:
        system += f"\n\nBrand knowledge:\n{bk}"

    model = get_kv("claude_model", "claude-haiku-4-5-20251001")
    prompt_template = get_kv("linkedin_prompt", LINKEDIN_PROMPT)
    try:
        prompt = prompt_template.format(post_name=post_name, properties=properties, content=content)
    except (KeyError, ValueError):
        prompt = f"{prompt_template}\n\nPost name: {post_name}\n\nProperties:\n{properties}\n\nContent:\n{content}"

    message = client.messages.create(
        model=model,
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


# ---------------------------------------------------------------------------
# Blotato
# ---------------------------------------------------------------------------
async def post_to_linkedin_via_blotato(content: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://backend.blotato.com/v2/posts",
            headers={
                "blotato-api-key": BLOTATO_API_KEY,
                "Content-Type": "application/json",
            },
            json={
                "post": {
                    "accountId": BLOTATO_LINKEDIN_ACCOUNT_ID,
                    "content": {
                        "text": content,
                        "mediaUrls": [],
                        "platform": "linkedin",
                    },
                    "target": {"targetType": "linkedin"},
                }
            },
        )
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------
async def notify_slack(review_id: str, post_name: str, preview: str):
    if not SLACK_WEBHOOK_URL:
        return
    preview_short = preview[:500] + ("…" if len(preview) > 500 else "")
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":pencil: *LinkedIn Post Ready for Review*\n*{post_name}*",
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```{preview_short}```"},
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✓ Approve & Post"},
                    "style": "primary",
                    "action_id": "approve",
                    "value": review_id,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✗ Reject"},
                    "style": "danger",
                    "action_id": "reject",
                    "value": review_id,
                },
            ],
        },
    ]
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            SLACK_WEBHOOK_URL,
            json={"text": f"LinkedIn post ready: {post_name}", "blocks": blocks},
        )


async def _do_approve(review_id: str, content: str | None = None) -> tuple[bool, str]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM pending_reviews WHERE id=?", (review_id,)).fetchone()
    if not row or row["status"] != "pending":
        return False, "Not found or already processed"
    final_content = content or row["post_content"]
    if content:
        with get_db() as conn:
            conn.execute(
                "UPDATE pending_reviews SET post_content=? WHERE id=?", (content, review_id)
            )
    try:
        await post_to_linkedin_via_blotato(final_content)
    except Exception as e:
        return False, f"Blotato error: {e}"
    try:
        await mark_notion_page_posted(row["notion_page_id"])
    except Exception as e:
        return False, f"Notion error (post WAS sent): {e}"
    with get_db() as conn:
        conn.execute("UPDATE pending_reviews SET status='approved' WHERE id=?", (review_id,))
    return True, ""


async def _do_reject(review_id: str) -> tuple[bool, str]:
    with get_db() as conn:
        conn.execute("UPDATE pending_reviews SET status='rejected' WHERE id=?", (review_id,))
    return True, ""


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
async def run_workflow() -> str:
    print(f"[{datetime.now()}] Running LinkedIn post workflow...")

    page = await find_notion_page()
    if not page:
        msg = "No pages found with Status=Not Started and ClientsOS=LMTZ."
        print(f"  {msg}")
        return msg

    page_id = page["id"]
    post_name = _extract_page_name(page)
    print(f"  Page: {post_name!r} ({page_id})")

    properties = await get_page_properties_as_text(page)
    content = await get_page_blocks_as_text(page_id)

    mock = get_mock_mode()
    mock_text = mock["text"] if mock["enabled"] else ""
    post_text = generate_linkedin_post(post_name, properties, content, mock_text=mock_text)
    label = "[MOCK]" if mock_text else ""
    print(f"  Generated {len(post_text)} chars of copy. {label}")

    await append_to_notion_page(page_id, post_text)

    review_id = str(uuid.uuid4())
    with get_db() as conn:
        conn.execute(
            "INSERT INTO pending_reviews (id, notion_page_id, post_name, post_content, status, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (review_id, page_id, post_name, post_text, "pending",
             datetime.now(timezone.utc).isoformat()),
        )

    await notify_slack(review_id, post_name, post_text)
    msg = f"Post generated for '{post_name}'. Review: {BASE_URL}/review/{review_id}"
    print(f"  {msg}")
    return msg


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
_scheduler: AsyncIOScheduler | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    init_db()
    h = int(get_kv("schedule_hour", str(SCHEDULE_HOUR)))
    m = int(get_kv("schedule_minute", str(SCHEDULE_MINUTE)))
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(run_workflow, "cron", id="daily_run", hour=h, minute=m)
    _scheduler.start()
    print(f"Scheduler started — runs daily at {h:02d}:{m:02d} UTC")
    yield
    _scheduler.shutdown()


app = FastAPI(title="LinkedIn Post Automation", lifespan=lifespan)

# ---------------------------------------------------------------------------
# HTML — CSS served as separate endpoint, JS uses real braces (no format())
# ---------------------------------------------------------------------------
_KNIGHTS_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;600;700;900&family=Rajdhani:wght@400;500;600&family=Share+Tech+Mono&display=swap');

*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:       #08080a;
  --panel:    #0d0d12;
  --border:   rgba(227,160,40,.18);
  --amber:    #e3a028;
  --amber-dim:#a06810;
  --green:    #28e060;
  --red:      #e04028;
  --blue:     #28a0e0;
  --text:     #d4b896;
  --text-dim: #7a6040;
  --mono:     'Share Tech Mono', monospace;
  --head:     'Orbitron', sans-serif;
  --body:     'Rajdhani', sans-serif;
}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--body);font-size:15px;line-height:1.5}
::-webkit-scrollbar{width:3px;height:3px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--amber-dim)}

/* LAYOUT */
.shell{display:flex;height:100vh;overflow:hidden}
.sidebar{width:220px;flex-shrink:0;background:var(--panel);border-right:1px solid var(--border);display:flex;flex-direction:column;padding:0}
.sidebar-logo{padding:24px 20px 16px;border-bottom:1px solid var(--border)}
.sidebar-logo .wordmark{font-family:var(--head);font-size:.7rem;letter-spacing:.25em;text-transform:uppercase;color:var(--amber);line-height:1.2}
.sidebar-logo .sub{font-size:.65rem;color:var(--text-dim);letter-spacing:.1em;margin-top:2px;font-family:var(--mono)}
.sidebar-nav{flex:1;padding:16px 0}
.nav-item{display:flex;align-items:center;gap:10px;padding:10px 20px;cursor:pointer;font-family:var(--head);font-size:.65rem;letter-spacing:.15em;text-transform:uppercase;color:var(--text-dim);border-left:2px solid transparent;transition:all .15s}
.nav-item:hover{color:var(--amber);background:rgba(227,160,40,.05)}
.nav-item.active{color:var(--amber);border-left-color:var(--amber);background:rgba(227,160,40,.08)}
.nav-icon{font-size:.9rem;width:18px;text-align:center}
.sidebar-footer{padding:16px 20px;border-top:1px solid var(--border)}
.run-btn{width:100%;background:var(--amber);color:#08080a;border:none;padding:10px;font-family:var(--head);font-size:.65rem;letter-spacing:.15em;text-transform:uppercase;cursor:pointer;font-weight:700;transition:opacity .15s}
.run-btn:hover{opacity:.85}
.run-btn:disabled{opacity:.4;cursor:not-allowed}
.run-status{font-family:var(--mono);font-size:.7rem;color:var(--text-dim);margin-top:8px;min-height:1rem;text-align:center;word-break:break-word}

/* MAIN */
.main{flex:1;overflow:hidden;display:flex;flex-direction:column}
.topbar{padding:0 28px;height:48px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
.topbar-left{font-family:var(--head);font-size:.7rem;letter-spacing:.2em;text-transform:uppercase;color:var(--amber)}
.topbar-right{display:flex;align-items:center;gap:20px;font-family:var(--mono);font-size:.7rem;color:var(--text-dim)}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:5px}
.dot-green{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot-amber{background:var(--amber);animation:pulse 1.5s infinite}
.dot-red{background:var(--red)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.content{flex:1;overflow-y:auto;padding:28px;display:flex;flex-direction:column}
.section{display:none}
.section.active{display:block}
#section-flow{flex:1;display:flex;flex-direction:column;overflow:hidden;margin:-28px}

/* STAT CARDS */
.stat-row{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:24px}
.stat-card{background:var(--panel);border:1px solid var(--border);padding:16px 20px}
.stat-num{font-family:var(--head);font-size:1.8rem;font-weight:700;color:var(--amber);line-height:1}
.stat-label{font-size:.7rem;letter-spacing:.12em;text-transform:uppercase;color:var(--text-dim);margin-top:4px;font-family:var(--mono)}

/* TABLE */
.table-wrap{background:var(--panel);border:1px solid var(--border);overflow:hidden}
.table-header{padding:12px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.table-title{font-family:var(--head);font-size:.65rem;letter-spacing:.2em;text-transform:uppercase;color:var(--amber)}
.table-refresh{background:none;border:1px solid var(--border);color:var(--text-dim);padding:4px 12px;font-family:var(--mono);font-size:.7rem;cursor:pointer;transition:all .15s}
.table-refresh:hover{border-color:var(--amber);color:var(--amber)}
table{width:100%;border-collapse:collapse}
th{padding:9px 16px;text-align:left;font-family:var(--mono);font-size:.65rem;letter-spacing:.1em;text-transform:uppercase;color:var(--text-dim);border-bottom:1px solid var(--border)}
td{padding:11px 16px;font-family:var(--body);font-size:.9rem;border-bottom:1px solid rgba(227,160,40,.06)}
tr:hover td{background:rgba(227,160,40,.03)}
tr:last-child td{border-bottom:none}
.badge{display:inline-block;padding:2px 10px;font-family:var(--mono);font-size:.65rem;letter-spacing:.08em;text-transform:uppercase;border:1px solid}
.badge-pending{color:#f0b030;border-color:#f0b030;background:rgba(240,176,48,.08)}
.badge-approved{color:var(--green);border-color:var(--green);background:rgba(40,224,96,.08)}
.badge-rejected{color:var(--red);border-color:var(--red);background:rgba(224,64,40,.08)}
.review-link{color:var(--amber);text-decoration:none;font-family:var(--mono);font-size:.75rem;letter-spacing:.05em;border-bottom:1px solid var(--amber-dim);transition:border-color .15s}
.review-link:hover{border-color:var(--amber)}
.empty-row td{text-align:center;color:var(--text-dim);font-family:var(--mono);font-size:.8rem;padding:48px}

/* DEBUG */
.debug-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
.debug-card{background:var(--panel);border:1px solid var(--border);padding:0;overflow:hidden}
.debug-card-head{padding:10px 16px;border-bottom:1px solid var(--border);font-family:var(--head);font-size:.62rem;letter-spacing:.18em;text-transform:uppercase;color:var(--amber)}
.debug-card-body{padding:14px 16px;font-family:var(--mono);font-size:.75rem;color:var(--text-dim);line-height:1.6}
.debug-card-body span{color:var(--text)}
.notion-raw{background:var(--panel);border:1px solid var(--border);overflow:hidden}
.notion-raw-head{padding:10px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.notion-raw-title{font-family:var(--head);font-size:.62rem;letter-spacing:.18em;text-transform:uppercase;color:var(--amber)}
pre.notion-pre{padding:16px;font-family:var(--mono);font-size:.72rem;color:var(--text-dim);overflow-x:auto;line-height:1.6;max-height:400px;overflow-y:auto}
pre.notion-pre .key{color:#e3a028}
pre.notion-pre .str{color:#c0dda0}
pre.notion-pre .num{color:#80c0e0}

/* SETTINGS */
.settings-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.settings-card{background:var(--panel);border:1px solid var(--border);overflow:hidden}
.settings-card-head{padding:12px 18px;border-bottom:1px solid var(--border);font-family:var(--head);font-size:.62rem;letter-spacing:.18em;text-transform:uppercase;color:var(--amber)}
.settings-card-body{padding:16px 18px;display:flex;flex-direction:column;gap:12px}
.field{display:flex;flex-direction:column;gap:4px}
.field label{font-family:var(--mono);font-size:.68rem;letter-spacing:.1em;text-transform:uppercase;color:var(--text-dim)}
.field-val{font-family:var(--mono);font-size:.8rem;color:var(--text);background:rgba(0,0,0,.3);border:1px solid var(--border);padding:7px 10px;word-break:break-all}
.field-val.ok{border-color:rgba(40,224,96,.3);color:var(--green)}
.field-val.missing{border-color:rgba(224,64,40,.3);color:var(--red)}
.prompt-area{width:100%;background:rgba(0,0,0,.3);border:1px solid var(--border);color:var(--text);font-family:var(--mono);font-size:.75rem;padding:10px;resize:vertical;min-height:200px;line-height:1.55}
.prompt-area:focus{outline:none;border-color:var(--amber)}
.save-btn{align-self:flex-start;background:none;border:1px solid var(--amber);color:var(--amber);padding:7px 20px;font-family:var(--head);font-size:.62rem;letter-spacing:.15em;text-transform:uppercase;cursor:pointer;transition:all .15s}
.save-btn:hover{background:rgba(227,160,40,.1)}
.save-msg{font-family:var(--mono);font-size:.72rem;color:var(--green);display:none}
.full-width{grid-column:1/-1}

/* TOGGLE */
.toggle{position:relative;display:inline-block;width:40px;height:22px;flex-shrink:0}
.toggle input{opacity:0;width:0;height:0;position:absolute}
.toggle-slider{position:absolute;cursor:pointer;inset:0;background:rgba(255,255,255,.07);border:1px solid var(--border);transition:.2s}
.toggle-slider:before{content:'';position:absolute;width:16px;height:16px;left:2px;bottom:2px;background:var(--text-dim);transition:.2s}
input:checked + .toggle-slider{background:rgba(227,160,40,.15);border-color:var(--amber)}
input:checked + .toggle-slider:before{transform:translateX(18px);background:var(--amber)}
.mock-badge{display:none;font-family:var(--mono);font-size:.62rem;letter-spacing:.1em;text-transform:uppercase;color:var(--red);border:1px solid var(--red);padding:1px 8px;animation:pulse 1.5s infinite}

/* MODAL */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:100;display:none;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal{background:var(--panel);border:1px solid var(--border);width:min(680px,95vw);max-height:85vh;display:flex;flex-direction:column}
.modal-head{padding:14px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
.modal-head-title{font-family:var(--head);font-size:.65rem;letter-spacing:.2em;text-transform:uppercase;color:var(--amber)}
.modal-close{background:none;border:none;color:var(--text-dim);font-size:1.2rem;cursor:pointer;line-height:1;padding:0 4px}
.modal-close:hover{color:var(--amber)}
.modal-name{padding:14px 20px 0;font-family:var(--body);font-size:1.05rem;color:var(--text);flex-shrink:0}
.modal-post{flex:1;overflow-y:auto;margin:12px 20px;white-space:pre-wrap;background:rgba(0,0,0,.3);border:1px solid var(--border);padding:16px;font-family:var(--body);font-size:.9rem;line-height:1.7;color:var(--text);resize:none;width:calc(100% - 40px);outline:none;min-height:120px}
.modal-post:focus{border-color:rgba(227,160,40,.4)}
.modal-actions{padding:14px 20px;border-top:1px solid var(--border);display:flex;gap:10px;flex-shrink:0}
.btn-approve{background:var(--amber);color:#08080a;border:none;padding:10px 24px;font-family:var(--head);font-size:.62rem;letter-spacing:.15em;text-transform:uppercase;cursor:pointer;font-weight:700;transition:opacity .15s}
.btn-approve:hover{opacity:.85}
.btn-approve:disabled{opacity:.4;cursor:not-allowed}
.btn-reject{background:none;border:1px solid var(--red);color:var(--red);padding:10px 24px;font-family:var(--head);font-size:.62rem;letter-spacing:.15em;text-transform:uppercase;cursor:pointer;transition:all .15s}
.btn-reject:hover{background:rgba(224,64,40,.1)}
.btn-reject:disabled{opacity:.4;cursor:not-allowed}
.modal-msg{font-family:var(--mono);font-size:.75rem;color:var(--text-dim);margin-left:auto;align-self:center}

/* FLOW CANVAS */
.flow-outer{flex:1;display:flex;flex-direction:column}
.flow-toolbar{padding:10px 24px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px;flex-shrink:0}
.flow-tool-btn{background:none;border:1px solid var(--border);color:var(--text-dim);padding:4px 14px;font-family:var(--mono);font-size:.68rem;cursor:pointer;transition:all .15s}
.flow-tool-btn:hover{border-color:var(--amber);color:var(--amber)}
.flow-inner{flex:1;display:flex;overflow:hidden;position:relative}
.flow-wrap{flex:1;overflow:hidden;background-color:var(--bg);background-image:radial-gradient(circle,rgba(227,160,40,.07) 1px,transparent 1px);background-size:28px 28px;cursor:grab;position:relative}
.flow-wrap.panning{cursor:grabbing}
.flow-canvas{position:absolute;transform-origin:0 0}
.flow-svg{position:absolute;top:0;left:0;pointer-events:none;overflow:visible}
.flow-node{position:absolute;width:192px;background:var(--panel);border:1px solid var(--border);cursor:default;transition:border-color .15s,box-shadow .15s;user-select:none}
.flow-node:hover{border-color:rgba(227,160,40,.45)}
.flow-node.selected{border-color:var(--amber) !important;box-shadow:0 0 0 1px rgba(227,160,40,.25)}
.fn-head{padding:9px 13px;display:flex;align-items:center;gap:8px;cursor:grab;border-bottom:1px solid var(--border)}
.fn-head:active{cursor:grabbing}
.fn-icon{font-size:.9rem;width:18px;text-align:center}
.fn-title{font-family:var(--head);font-size:.58rem;letter-spacing:.15em;text-transform:uppercase;flex:1}
.fn-dot{width:7px;height:7px;border-radius:50%;background:var(--text-dim);flex-shrink:0}
.fn-dot.live{background:var(--green);box-shadow:0 0 5px var(--green)}
.fn-body{padding:10px 13px;display:flex;flex-direction:column;gap:5px}
.fn-row{display:flex;flex-direction:column;gap:1px}
.fn-lbl{font-family:var(--mono);font-size:.58rem;letter-spacing:.07em;text-transform:uppercase;color:var(--text-dim)}
.fn-val{font-family:var(--mono);font-size:.7rem;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.fn-port{position:absolute;width:10px;height:10px;background:var(--panel);border:2px solid var(--amber-dim);border-radius:50%;top:calc(50% - 5px)}
.fn-port-in{left:-5px}
.fn-port-out{right:-5px}
.flow-cfg{width:0;overflow:hidden;background:var(--panel);border-left:1px solid var(--border);transition:width .2s;display:flex;flex-direction:column;flex-shrink:0}
.flow-cfg.open{width:264px}
.fcfg-head{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-shrink:0;min-width:264px}
.fcfg-title{font-family:var(--head);font-size:.6rem;letter-spacing:.18em;text-transform:uppercase;color:var(--amber)}
.fcfg-close{background:none;border:none;color:var(--text-dim);cursor:pointer;font-size:1rem;padding:0 2px}
.fcfg-close:hover{color:var(--amber)}
.fcfg-body{padding:14px 16px;display:flex;flex-direction:column;gap:12px;overflow-y:auto;flex:1;min-width:264px}
.fcfg-desc{font-family:var(--body);font-size:.82rem;color:var(--text-dim);line-height:1.5;padding-bottom:10px;border-bottom:1px solid var(--border)}
.fcfg-field{display:flex;flex-direction:column;gap:4px}
.fcfg-lbl{font-family:var(--mono);font-size:.62rem;letter-spacing:.09em;text-transform:uppercase;color:var(--text-dim)}
.fcfg-val{font-family:var(--mono);font-size:.74rem;color:var(--text);background:rgba(0,0,0,.25);border:1px solid var(--border);padding:6px 9px;word-break:break-all;line-height:1.4}
.fcfg-textarea{background:rgba(0,0,0,.3);border:1px solid var(--border);color:var(--text);font-family:var(--mono);font-size:.7rem;padding:7px 9px;width:100%;outline:none;resize:vertical;min-height:90px;line-height:1.5;box-sizing:border-box}
.fcfg-textarea:focus{border-color:var(--amber)}
.fcfg-btn{background:none;border:1px solid var(--amber);color:var(--amber);padding:6px 16px;font-family:var(--head);font-size:.58rem;letter-spacing:.15em;text-transform:uppercase;cursor:pointer;transition:all .15s}
.fcfg-btn:hover{background:rgba(227,160,40,.1)}
.fcfg-saved{font-family:var(--mono);font-size:.7rem;color:var(--green);display:none;margin-left:8px}
.fcfg-input{background:rgba(0,0,0,.35);border:1px solid var(--border);color:var(--text);font-family:var(--mono);font-size:.72rem;padding:6px 9px;width:100%;outline:none;box-sizing:border-box}
.fcfg-input:focus{border-color:var(--amber)}
.fcfg-input option{background:var(--panel);color:var(--text)}
.fcfg-btn-row{display:flex;align-items:center;gap:10px;padding-top:4px}

@media(max-width:700px){
  .sidebar{width:56px}
  .sidebar-logo .wordmark,.sidebar-logo .sub,.nav-item span,.run-btn,.run-status,.sidebar-footer .run-btn-label{display:none}
  .nav-item{justify-content:center;padding:12px}
  .stat-row{grid-template-columns:1fr}
  .debug-grid,.settings-grid{grid-template-columns:1fr}
  .topbar-right{display:none}
}
"""

_DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AMEZCLA // LinkedIn OPS</title>
<style>__CSS__</style>
</head>
<body>
<div class="shell">

<!-- SIDEBAR -->
<aside class="sidebar">
  <div class="sidebar-logo">
    <div class="wordmark">AMEZCLA</div>
    <div class="sub">LinkedIn OPS</div>
  </div>
  <nav class="sidebar-nav">
    <div class="nav-item active" data-section="flow" onclick="switchTab('flow',this)">
      <span class="nav-icon">◈</span><span>Flow</span>
    </div>
    <div class="nav-item" data-section="posts" onclick="switchTab('posts',this)">
      <span class="nav-icon">▤</span><span>Posts</span>
    </div>
    <div class="nav-item" data-section="debug" onclick="switchTab('debug',this)">
      <span class="nav-icon">⬡</span><span>Debug</span>
    </div>
    <div class="nav-item" data-section="settings" onclick="switchTab('settings',this)">
      <span class="nav-icon">⚙</span><span>Settings</span>
    </div>
  </nav>
  <div class="sidebar-footer">
    <button class="run-btn" id="runBtn" onclick="runNow()">▶ RUN NOW</button>
    <div class="run-status" id="runStatus"></div>
  </div>
</aside>

<!-- MAIN -->
<div class="main">
  <div class="topbar">
    <span class="topbar-left">Command // LinkedIn</span>
    <div class="topbar-right">
      <span><span class="dot dot-green"></span>ONLINE</span>
      <span>NEXT RUN __NEXT_RUN__</span>
      <span class="mock-badge" id="mock-badge">TEST MODE</span>
      <span id="clock"></span>
    </div>
  </div>

  <div class="content">

    <!-- FLOW -->
    <div class="section active" id="section-flow">
      <div class="flow-outer">
        <div class="flow-toolbar">
          <span style="font-family:var(--head);font-size:.6rem;letter-spacing:.2em;text-transform:uppercase;color:var(--amber)">Workflow</span>
          <button class="flow-tool-btn" onclick="resetFlowView()">⊡ Reset View</button>
          <button class="flow-tool-btn" onclick="runNow()">▶ Run Now</button>
          <span id="flow-run-msg" style="font-family:var(--mono);font-size:.7rem;color:var(--text-dim)"></span>
        </div>
        <div class="flow-inner">
          <div class="flow-wrap" id="flow-wrap">
            <div class="flow-canvas" id="flow-canvas">
              <svg class="flow-svg" id="flow-svg"></svg>
            </div>
          </div>
          <div class="flow-cfg" id="flow-cfg">
            <div class="fcfg-head">
              <span class="fcfg-title" id="fcfg-title">Node</span>
              <button class="fcfg-close" onclick="closeCfg()">✕</button>
            </div>
            <div class="fcfg-body" id="fcfg-body"></div>
          </div>
        </div>
      </div>
    </div>

    <!-- POSTS -->
    <div class="section" id="section-posts">
      <div class="stat-row">
        <div class="stat-card">
          <div class="stat-num" id="cnt-pending">—</div>
          <div class="stat-label">Awaiting Review</div>
        </div>
        <div class="stat-card">
          <div class="stat-num" id="cnt-approved">—</div>
          <div class="stat-label">Posted</div>
        </div>
        <div class="stat-card">
          <div class="stat-num" id="cnt-rejected">—</div>
          <div class="stat-label">Rejected</div>
        </div>
      </div>
      <div class="table-wrap">
        <div class="table-header">
          <span class="table-title">Post Queue</span>
          <button class="table-refresh" onclick="loadPosts()">↺ Refresh</button>
        </div>
        <table>
          <thead><tr><th>Post</th><th>Status</th><th>Created</th><th></th></tr></thead>
          <tbody id="posts-tbody"><tr class="empty-row"><td colspan="4">Loading...</td></tr></tbody>
        </table>
      </div>
    </div>

    <!-- DEBUG -->
    <div class="section" id="section-debug">
      <div class="debug-grid">
        <div class="debug-card">
          <div class="debug-card-head">Config</div>
          <div class="debug-card-body">
            <div>DB ID &nbsp;&nbsp;<span>__NOTION_DB_ID__</span></div>
            <div>LMTZ ID &nbsp;<span>__LMTZ_PAGE_ID__</span></div>
            <div>Schedule <span>__NEXT_RUN__ UTC</span></div>
            <div>Base URL <span>__BASE_URL__</span></div>
          </div>
        </div>
        <div class="debug-card">
          <div class="debug-card-head">API Status</div>
          <div class="debug-card-body" id="api-status">Checking...</div>
        </div>
      </div>
      <div class="notion-raw">
        <div class="notion-raw-head">
          <span class="notion-raw-title">Raw Notion Query (first 3 rows, no filter)</span>
          <button class="table-refresh" onclick="loadDebug()">↺ Refresh</button>
        </div>
        <pre class="notion-pre" id="notion-raw-pre">Loading...</pre>
      </div>
    </div>

    <!-- SETTINGS -->
    <div class="section" id="section-settings">
      <div class="settings-grid">
        <div class="settings-card">
          <div class="settings-card-head">Environment</div>
          <div class="settings-card-body" id="env-fields">Loading...</div>
        </div>
        <div class="settings-card">
          <div class="settings-card-head">Schedule</div>
          <div class="settings-card-body">
            <div class="field"><label>Run Time (UTC)</label><div class="field-val">__NEXT_RUN__</div></div>
            <div class="field"><label>Frequency</label><div class="field-val">Daily</div></div>
            <div class="field"><label>To change</label><div class="field-val" style="font-size:.7rem">Update SCHEDULE_HOUR / SCHEDULE_MINUTE in Render env vars</div></div>
          </div>
        </div>
        <div class="settings-card full-width">
          <div class="settings-card-head">Test Mode — Skip Claude</div>
          <div class="settings-card-body">
            <div class="field">
              <label>Bypass Claude API (no credits used)</label>
              <div style="display:flex;align-items:center;gap:12px;margin-top:8px">
                <label class="toggle">
                  <input type="checkbox" id="mock-toggle" onchange="saveMockMode()">
                  <span class="toggle-slider"></span>
                </label>
                <span id="mock-label" style="font-family:var(--mono);font-size:.8rem;color:var(--text-dim)">OFF</span>
              </div>
            </div>
            <div class="field" id="mock-text-field" style="display:none">
              <label>Mock post text (used instead of Claude output)</label>
              <textarea class="prompt-area" id="mock-text" style="min-height:80px" placeholder="This is a test post..."></textarea>
            </div>
            <div style="display:flex;align-items:center;gap:12px">
              <button class="save-btn" onclick="saveMockMode()">Save</button>
              <span class="save-msg" id="mock-save-msg">Saved ✓</span>
            </div>
          </div>
        </div>
        <div class="settings-card full-width">
          <div class="settings-card-head">Brand Knowledge / System Prompt Override</div>
          <div class="settings-card-body">
            <div class="field">
              <label>Append to Claude system prompt</label>
              <textarea class="prompt-area" id="brand-knowledge" placeholder="Paste brand guidelines, tone of voice, examples..."></textarea>
            </div>
            <div style="display:flex;align-items:center;gap:12px">
              <button class="save-btn" onclick="saveBrandKnowledge()">Save</button>
              <span class="save-msg" id="save-msg">Saved ✓</span>
            </div>
          </div>
        </div>
        <div class="settings-card full-width">
          <div class="settings-card-head">LinkedIn Post Prompt</div>
          <div class="settings-card-body">
            <div class="field">
              <label>Prompt template (use {post_name}, {properties}, {content} as placeholders)</label>
              <textarea class="prompt-area" id="linkedin-prompt" style="min-height:260px" placeholder="Loading..."></textarea>
            </div>
            <div style="display:flex;align-items:center;gap:12px">
              <button class="save-btn" onclick="savePrompt()">Save</button>
              <span class="save-msg" id="prompt-save-msg">Saved ✓</span>
            </div>
          </div>
        </div>
      </div>
    </div>

  </div><!-- /content -->
</div><!-- /main -->
</div><!-- /shell -->

<!-- REVIEW MODAL -->
<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="modal-head">
      <span class="modal-head-title">Review Post</span>
      <button class="modal-close" onclick="closeModal()">✕</button>
    </div>
    <div class="modal-name" id="modal-name"></div>
    <textarea class="modal-post" id="modal-post" spellcheck="true"></textarea>
    <div class="modal-actions">
      <button class="btn-approve" id="modal-approve" onclick="submitReview('approve')">✓ Approve &amp; Post</button>
      <button class="btn-reject"  id="modal-reject"  onclick="submitReview('reject')">✗ Reject</button>
      <span class="modal-msg" id="modal-msg"></span>
    </div>
  </div>
</div>

<script>
function switchTab(name, el) {
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('section-' + name).classList.add('active');
  el.classList.add('active');
  if (name === 'debug') loadDebug();
  if (name === 'settings') loadSettings();
}

// Clock
function tick() {
  const el = document.getElementById('clock');
  if (el) el.textContent = new Date().toUTCString().slice(17, 25) + ' UTC';
}
tick(); setInterval(tick, 1000);

// Posts
async function loadPosts() {
  const res = await fetch('/api/posts');
  const posts = await res.json();
  const tbody = document.getElementById('posts-tbody');
  let pending=0, approved=0, rejected=0;
  if (!posts.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="4">No posts yet — click Run Now to generate one.</td></tr>';
  } else {
    tbody.innerHTML = posts.map(p => {
      if(p.status==='pending') pending++;
      else if(p.status==='approved') approved++;
      else rejected++;
      const created = p.created_at ? p.created_at.slice(0,16).replace('T',' ') : '';
      const action = p.status === 'pending'
        ? `<a class="review-link" href="#" onclick="openReview('${p.id}','${(p.post_name||'').replace(/'/g,"\\'")}');return false">Review →</a>` : '';
      return `<tr>
        <td>${p.post_name || '(untitled)'}</td>
        <td><span class="badge badge-${p.status}">${p.status}</span></td>
        <td style="font-family:var(--mono);font-size:.75rem;color:var(--text-dim)">${created}</td>
        <td>${action}</td>
      </tr>`;
    }).join('');
    pending=posts.filter(p=>p.status==='pending').length;
    approved=posts.filter(p=>p.status==='approved').length;
    rejected=posts.filter(p=>p.status==='rejected').length;
  }
  document.getElementById('cnt-pending').textContent = pending;
  document.getElementById('cnt-approved').textContent = approved;
  document.getElementById('cnt-rejected').textContent = rejected;
}

// Debug
async function loadDebug() {
  document.getElementById('notion-raw-pre').textContent = 'Loading...';
  document.getElementById('api-status').innerHTML = 'Checking...';
  try {
    const [health, notion] = await Promise.all([
      fetch('/health').then(r => r.json()),
      fetch('/debug-notion').then(r => r.json()),
    ]);
    document.getElementById('api-status').innerHTML =
      `<div><span style="color:var(--green)">● ONLINE</span></div>` +
      `<div>Time &nbsp;<span>${health.time}</span></div>` +
      `<div>Posts &nbsp;<span>${notion.length} rows fetched</span></div>`;
    document.getElementById('notion-raw-pre').textContent =
      JSON.stringify(notion, null, 2);
  } catch(e) {
    document.getElementById('api-status').innerHTML = `<span style="color:var(--red)">Error: ${e}</span>`;
    document.getElementById('notion-raw-pre').textContent = 'Failed to load.';
  }
}

// Settings
async function loadSettings() {
  const res = await fetch('/api/config');
  const cfg = await res.json();
  const container = document.getElementById('env-fields');
  container.innerHTML = Object.entries(cfg).map(([k,v]) => {
    const cls = v === '✓ SET' ? 'ok' : v === '✗ MISSING' ? 'missing' : '';
    return `<div class="field"><label>${k}</label><div class="field-val ${cls}">${v}</div></div>`;
  }).join('');

  const bk = await fetch('/api/brand-knowledge').then(r=>r.json());
  document.getElementById('brand-knowledge').value = bk.value || '';
  await loadMockMode();
  await loadPrompt();
}

async function loadMockMode() {
  const data = await fetch('/api/mock-mode').then(r=>r.json());
  const toggle = document.getElementById('mock-toggle');
  toggle.checked = data.enabled;
  document.getElementById('mock-text').value = data.text || '';
  document.getElementById('mock-label').textContent = data.enabled ? 'ON' : 'OFF';
  document.getElementById('mock-label').style.color = data.enabled ? 'var(--amber)' : 'var(--text-dim)';
  document.getElementById('mock-text-field').style.display = data.enabled ? 'block' : 'none';
  document.getElementById('mock-badge').style.display = data.enabled ? 'inline' : 'none';
}

async function saveMockMode() {
  const enabled = document.getElementById('mock-toggle').checked;
  const text = document.getElementById('mock-text').value;
  document.getElementById('mock-label').textContent = enabled ? 'ON' : 'OFF';
  document.getElementById('mock-label').style.color = enabled ? 'var(--amber)' : 'var(--text-dim)';
  document.getElementById('mock-text-field').style.display = enabled ? 'block' : 'none';
  document.getElementById('mock-badge').style.display = enabled ? 'inline' : 'none';
  await fetch('/api/mock-mode', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({enabled, text})
  });
  const msg = document.getElementById('mock-save-msg');
  msg.style.display = 'inline';
  setTimeout(() => msg.style.display = 'none', 2000);
}

async function loadPrompt() {
  const cfg = await fetch('/api/node-config').then(r=>r.json());
  document.getElementById('linkedin-prompt').value = cfg.linkedin_prompt || '';
}

async function savePrompt() {
  const val = document.getElementById('linkedin-prompt').value;
  await fetch('/api/node-config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({linkedin_prompt: val})
  });
  const msg = document.getElementById('prompt-save-msg');
  msg.style.display = 'inline';
  setTimeout(() => msg.style.display = 'none', 2000);
}

async function saveBrandKnowledge() {
  const val = document.getElementById('brand-knowledge').value;
  await fetch('/api/brand-knowledge', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({value: val})
  });
  const msg = document.getElementById('save-msg');
  msg.style.display = 'inline';
  setTimeout(() => msg.style.display = 'none', 2000);
}

// Run Now
async function runNow() {
  const btn = document.getElementById('runBtn');
  const status = document.getElementById('runStatus');
  btn.disabled = true;
  status.textContent = 'Running...';
  const flowMsg = document.getElementById('flow-run-msg');
  try {
    const res = await fetch('/api/run', {method:'POST'});
    const data = await res.json();
    const msg = data.message || data.status;
    status.textContent = msg;
    if(flowMsg) flowMsg.textContent = msg;
    loadPosts();
  } catch(e) {
    status.textContent = 'Error: ' + e;
    if(flowMsg) flowMsg.textContent = 'Error: ' + e;
  }
  btn.disabled = false;
  setTimeout(() => { if(flowMsg) flowMsg.textContent=''; }, 8000);
}

// Modal
let _activeReviewId = null;

async function openReview(id, name) {
  _activeReviewId = id;
  document.getElementById('modal-name').textContent = name;
  document.getElementById('modal-post').value = 'Loading...';
  document.getElementById('modal-msg').textContent = '';
  document.getElementById('modal-approve').disabled = false;
  document.getElementById('modal-reject').disabled = false;
  document.getElementById('modal').classList.add('open');
  const res = await fetch('/api/review/' + id);
  const data = await res.json();
  document.getElementById('modal-post').value = data.post_content || '(empty)';
}

function closeModal() {
  document.getElementById('modal').classList.remove('open');
  _activeReviewId = null;
}

async function submitReview(action) {
  if (!_activeReviewId) return;
  document.getElementById('modal-approve').disabled = true;
  document.getElementById('modal-reject').disabled = true;
  document.getElementById('modal-msg').textContent = 'Processing...';
  const content = document.getElementById('modal-post').value;
  const res = await fetch('/review/' + _activeReviewId + '/' + action, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({content})
  });
  const text = await res.text();
  if (text.includes('error') || text.includes('Error')) {
    document.getElementById('modal-msg').style.color = 'var(--red)';
    document.getElementById('modal-msg').textContent = 'Error — see details below';
    document.getElementById('modal-post').value = text.replace(/<[^>]*>/g,'').trim();
    document.getElementById('modal-approve').disabled = false;
    document.getElementById('modal-reject').disabled = false;
  } else {
    document.getElementById('modal-msg').style.color = 'var(--green)';
    document.getElementById('modal-msg').textContent = action === 'approve' ? '✓ Posted to LinkedIn!' : '✗ Rejected';
    setTimeout(() => { closeModal(); loadPosts(); }, 1500);
  }
}

document.addEventListener('keydown', e => { if(e.key==='Escape') { closeModal(); closeCfg(); } });

// ─── FLOW CANVAS ─────────────────────────────────────────────────────────────
const FN_W = 192;

const NODES = [
  { id:'trigger',    title:'Schedule',      icon:'⏰', color:'#28a0e0',
    fields:[{l:'Type',v:'Daily Cron'},{l:'Time (UTC)',v:'__NEXT_RUN__'}],
    out:true },
  { id:'notion-in',  title:'Notion Query',  icon:'▤',  color:'#e3a028',
    fields:[{l:'Filter',v:'Not Started + LMTZ'},{l:'Returns',v:'1 page'}],
    inp:true, out:true },
  { id:'claude',     title:'Claude AI',     icon:'✦',  color:'#9b59b6',
    fields:[{l:'Model',v:'Haiku 4.5'},{l:'Output',v:'LinkedIn copy'}],
    inp:true, out:true },
  { id:'review',     title:'Human Review',  icon:'◉',  color:'#e08028',
    fields:[{l:'Gate',v:'Approve / Edit / Reject'}],
    inp:true, out:true },
  { id:'blotato',    title:'Blotato Post',  icon:'↗',  color:'#28e060',
    fields:[{l:'Platform',v:'LinkedIn'},{l:'API',v:'blotato.com/v2'}],
    inp:true, out:true },
  { id:'notion-out', title:'Notion Update', icon:'✓',  color:'#e3a028',
    fields:[{l:'Status →',v:'Posted 🎉'},{l:'Sets',v:'Posting Date, Type'}],
    inp:true },
];

const EDGES = [
  ['trigger','notion-in'],['notion-in','claude'],
  ['claude','review'],['review','blotato'],['blotato','notion-out'],
];

const NODE_CFG = {
  'trigger': { desc:'Fires the workflow on a daily cron schedule, or immediately via Run Now.',
    fields:[
      {l:'Run time (UTC)', key:'schedule_time', type:'time'},
      {l:'Trigger type',   v:'Daily Cron', readonly:true}
    ]},
  'notion-in': { desc:'Queries the Notion database for the next unprocessed LMTZ post.',
    fields:[
      {l:'Database ID',   key:'notion_db_id', type:'text'},
      {l:'Status filter', v:'Not Started', readonly:true},
      {l:'LMTZ Page ID',  key:'lmtz_page_id', type:'text'}
    ]},
  'claude': { desc:'Generates LinkedIn post copy using Claude AI.',
    fields:[
      {l:'Model', key:'claude_model', type:'select',
       options:['claude-haiku-4-5-20251001','claude-sonnet-5','claude-opus-4-8']},
      {l:'LinkedIn Prompt', key:'linkedin_prompt', type:'textarea'}
    ]},
  'review': { desc:'Pauses for human review. Edit the copy inline in the modal before approving.',
    fields:[{l:'UI', v:'Dashboard modal (editable)', readonly:true}], dynamic:'pending' },
  'blotato': { desc:'Posts the approved copy to LinkedIn via the Blotato API.',
    fields:[
      {l:'Endpoint', v:'backend.blotato.com/v2/posts', readonly:true},
      {l:'Platform',  v:'LinkedIn', readonly:true}
    ]},
  'notion-out': { desc:'Marks the Notion page as Posted and records the posting date.',
    fields:[
      {l:'Status →',    v:'Posted 🎉', readonly:true},
      {l:'Posting Date',v:'Set to UTC now', readonly:true},
      {l:'Type',        v:'Written Post', readonly:true}
    ]},
};

let flowPan={x:60,y:80}, flowScale=0.85;
let isPanning=false, panStart={};
let isDragging=false, dragId=null, dragOff={};

function fnHeight(n){ return 42 + n.fields.length * 28 + 8; }

function initFlow() {
  const wrap = document.getElementById('flow-wrap');
  const canvas = document.getElementById('flow-canvas');
  let saved = {};
  try { saved = JSON.parse(localStorage.getItem('flow_pos') || '{}'); } catch(e) {}

  let dx = 40;
  NODES.forEach(n => {
    n.x = saved[n.id]?.x ?? dx;
    n.y = saved[n.id]?.y ?? 80;
    dx += FN_W + 56;
    const el = document.createElement('div');
    el.className = 'flow-node'; el.id = 'fn-' + n.id;
    el.style.cssText = `left:${n.x}px;top:${n.y}px`;
    el.innerHTML = `
      <div class="fn-head" style="border-left:3px solid ${n.color}">
        <span class="fn-icon">${n.icon}</span>
        <span class="fn-title" style="color:${n.color}">${n.title}</span>
        <span class="fn-dot" id="fd-${n.id}"></span>
      </div>
      <div class="fn-body">
        ${n.fields.map(f=>`<div class="fn-row"><span class="fn-lbl">${f.l}</span><span class="fn-val">${f.v}</span></div>`).join('')}
      </div>
      ${n.inp ? '<div class="fn-port fn-port-in"></div>' : ''}
      ${n.out ? '<div class="fn-port fn-port-out"></div>' : ''}`;
    el.querySelector('.fn-head').addEventListener('mousedown', e => {
      e.stopPropagation();
      isDragging=true; dragId=n.id;
      dragOff = { x: e.clientX - n.x*flowScale - flowPan.x,
                  y: e.clientY - n.y*flowScale - flowPan.y };
      selectFn(n.id);
    });
    el.addEventListener('click', e => { e.stopPropagation(); selectFn(n.id); });
    canvas.appendChild(el);
  });

  drawEdges();
  applyFlowXform();

  wrap.addEventListener('mousedown', e => {
    if (!isDragging) {
      isPanning=true;
      panStart={x:e.clientX-flowPan.x, y:e.clientY-flowPan.y};
      wrap.classList.add('panning');
    }
  });
  window.addEventListener('mousemove', e => {
    if (isPanning) {
      flowPan.x=e.clientX-panStart.x; flowPan.y=e.clientY-panStart.y;
      applyFlowXform();
    }
    if (isDragging && dragId) {
      const n = NODES.find(n=>n.id===dragId);
      n.x=(e.clientX-dragOff.x-flowPan.x)/flowScale;
      n.y=(e.clientY-dragOff.y-flowPan.y)/flowScale;
      document.getElementById('fn-'+dragId).style.left=n.x+'px';
      document.getElementById('fn-'+dragId).style.top=n.y+'px';
      drawEdges();
    }
  });
  window.addEventListener('mouseup', () => {
    if (isPanning) { isPanning=false; wrap.classList.remove('panning'); }
    if (isDragging && dragId) {
      const n=NODES.find(n=>n.id===dragId);
      const s=JSON.parse(localStorage.getItem('flow_pos')||'{}');
      s[dragId]={x:n.x,y:n.y};
      localStorage.setItem('flow_pos',JSON.stringify(s));
      isDragging=false; dragId=null;
    }
  });
  wrap.addEventListener('wheel', e => {
    e.preventDefault();
    const d=e.deltaY<0?1.1:0.9;
    const r=wrap.getBoundingClientRect();
    const mx=e.clientX-r.left, my=e.clientY-r.top;
    flowPan.x=mx+(flowPan.x-mx)*d;
    flowPan.y=my+(flowPan.y-my)*d;
    flowScale=Math.min(2,Math.max(0.2,flowScale*d));
    applyFlowXform();
  },{passive:false});
}

function drawEdges() {
  const svg=document.getElementById('flow-svg');
  svg.innerHTML='<defs><marker id="arr" markerWidth="7" markerHeight="7" refX="5" refY="3" orient="auto"><polygon points="0 0,6 3,0 6" fill="rgba(227,160,40,.55)"/></marker></defs>';
  svg.setAttribute('width','4000'); svg.setAttribute('height','2000');
  EDGES.forEach(([fid,tid]) => {
    const fn=NODES.find(n=>n.id===fid), tn=NODES.find(n=>n.id===tid);
    if(!fn||!tn) return;
    const ax=fn.x+FN_W, ay=fn.y+fnHeight(fn)/2;
    const bx=tn.x,       by=tn.y+fnHeight(tn)/2;
    const cx=(ax+bx)/2;
    const p=document.createElementNS('http://www.w3.org/2000/svg','path');
    p.setAttribute('d',`M${ax},${ay} C${cx},${ay} ${cx},${by} ${bx},${by}`);
    p.setAttribute('stroke','rgba(227,160,40,.3)');
    p.setAttribute('stroke-width','2');
    p.setAttribute('fill','none');
    p.setAttribute('marker-end','url(#arr)');
    svg.appendChild(p);
  });
}

function applyFlowXform() {
  document.getElementById('flow-canvas').style.transform=
    `translate(${flowPan.x}px,${flowPan.y}px) scale(${flowScale})`;
}

function resetFlowView() {
  flowPan={x:60,y:80}; flowScale=0.85; applyFlowXform();
}

let selectedFnId=null;

function selectFn(id) {
  document.querySelectorAll('.flow-node').forEach(el=>el.classList.remove('selected'));
  document.getElementById('fn-'+id)?.classList.add('selected');
  selectedFnId=id; openCfg(id);
}

async function openCfg(id) {
  const cfg=NODE_CFG[id];
  const n=NODES.find(n=>n.id===id);
  if(!cfg||!n) return;
  document.getElementById('fcfg-title').textContent=n.title;

  let config={};
  try { config=await fetch('/api/node-config').then(r=>r.json()); } catch(e){}

  function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
  let html=`<div class="fcfg-desc">${cfg.desc}</div>`;
  let hasEditable=false;
  for(const f of cfg.fields) {
    if(f.readonly||!f.key) {
      html+=`<div class="fcfg-field"><span class="fcfg-lbl">${f.l}</span><div class="fcfg-val">${esc(f.v??'')}</div></div>`;
    } else {
      hasEditable=true;
      const raw=config[f.key]??f.v??'';
      if(f.type==='select') {
        const opts=f.options.map(o=>`<option value="${esc(o)}"${o===config[f.key]?' selected':''}>${esc(o)}</option>`).join('');
        html+=`<div class="fcfg-field"><span class="fcfg-lbl">${f.l}</span><select class="fcfg-input" data-key="${f.key}">${opts}</select></div>`;
      } else if(f.type==='textarea') {
        html+=`<div class="fcfg-field"><span class="fcfg-lbl">${f.l}</span><textarea class="fcfg-textarea fcfg-input" data-key="${f.key}" style="min-height:220px">${esc(raw)}</textarea></div>`;
      } else {
        html+=`<div class="fcfg-field"><span class="fcfg-lbl">${f.l}</span><input class="fcfg-input" type="${f.type||'text'}" data-key="${f.key}" value="${esc(raw)}"></div>`;
      }
    }
  }
  if(cfg.dynamic==='pending') {
    try {
      const posts=await fetch('/api/posts').then(r=>r.json());
      const p=posts.filter(p=>p.status==='pending').length;
      const a=posts.filter(p=>p.status==='approved').length;
      html+=`<div class="fcfg-field"><span class="fcfg-lbl">Pending reviews</span><div class="fcfg-val">${p}</div></div>`;
      html+=`<div class="fcfg-field"><span class="fcfg-lbl">Total approved</span><div class="fcfg-val">${a}</div></div>`;
    } catch(e){}
  }
  if(hasEditable) {
    html+=`<div class="fcfg-btn-row"><button class="fcfg-btn" onclick="saveCfg()">Save</button><span class="fcfg-saved" id="fcfg-saved-msg">Saved ✓</span></div>`;
  }
  if(cfg.btn) {
    html+=`<button class="fcfg-btn" style="margin-top:4px" onclick="${cfg.btn.fn}()">${cfg.btn.label}</button>`;
  }
  document.getElementById('fcfg-body').innerHTML=html;
  document.getElementById('flow-cfg').classList.add('open');
}

async function saveCfg() {
  const inputs=document.querySelectorAll('#fcfg-body .fcfg-input');
  const updates={};
  inputs.forEach(el=>{ if(el.dataset.key) updates[el.dataset.key]=el.value; });
  try {
    await fetch('/api/node-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(updates)});
    const msg=document.getElementById('fcfg-saved-msg');
    if(msg){msg.style.display='inline';setTimeout(()=>msg.style.display='none',2500);}
  } catch(e){ alert('Save failed: '+e.message); }
}

function goToSettings() {
  const el = document.querySelector('[data-section="settings"]');
  if (el) switchTab('settings', el);
}

function closeCfg() {
  document.getElementById('flow-cfg')?.classList.remove('open');
  document.querySelectorAll('.flow-node').forEach(el=>el.classList.remove('selected'));
  selectedFnId=null;
}

// Init
loadPosts();
setInterval(loadPosts, 30000);
initFlow();
</script>
</body></html>"""

_REVIEW_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Review // __POST_NAME__</title>
<style>__CSS__</style>
</head>
<body style="background:var(--bg)">
<div style="max-width:700px;margin:60px auto;padding:0 28px">
  <div style="font-family:var(--head);font-size:.65rem;letter-spacing:.2em;text-transform:uppercase;color:var(--amber);margin-bottom:12px">LinkedIn Post Review</div>
  <div style="font-family:var(--body);font-size:1.1rem;color:var(--text);margin-bottom:20px">__POST_NAME__</div>
  <div style="white-space:pre-wrap;background:rgba(0,0,0,.3);border:1px solid var(--border);padding:20px;font-family:var(--body);font-size:.9rem;line-height:1.7;color:var(--text);margin-bottom:20px">__POST_CONTENT__</div>
  <div style="display:flex;gap:12px">
    <form method="POST" action="/review/__REVIEW_ID__/approve">
      <button class="btn-approve" type="submit">✓ Approve &amp; Post</button>
    </form>
    <form method="POST" action="/review/__REVIEW_ID__/reject">
      <button class="btn-reject" type="submit">✗ Reject</button>
    </form>
  </div>
</div>
</body></html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    next_run = f"{SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}"
    return (
        _DASHBOARD_HTML
        .replace("__CSS__", _KNIGHTS_CSS)
        .replace("__NEXT_RUN__", next_run)
        .replace("__NOTION_DB_ID__", NOTION_DB_ID)
        .replace("__LMTZ_PAGE_ID__", LMTZ_PAGE_ID)
        .replace("__BASE_URL__", BASE_URL)
    )


@app.get("/api/posts")
async def api_posts():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, post_name, status, created_at FROM pending_reviews ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/config")
async def api_config():
    def masked(val):
        if not val:
            return "✗ MISSING"
        if len(val) > 12:
            return val[:6] + "..." + val[-4:]
        return "✓ SET"

    return {
        "NOTION_TOKEN":              masked(os.environ.get("NOTION_TOKEN", "")),
        "ANTHROPIC_API_KEY":         masked(os.environ.get("ANTHROPIC_API_KEY", "")),
        "BLOTATO_API_KEY":           masked(os.environ.get("BLOTATO_API_KEY", "")),
        "BLOTATO_LINKEDIN_ACCT_ID":  os.environ.get("BLOTATO_LINKEDIN_ACCOUNT_ID", "✗ MISSING"),
        "SLACK_WEBHOOK_URL":         "✓ SET" if os.environ.get("SLACK_WEBHOOK_URL") else "— not set",
        "BASE_URL":                  BASE_URL,
        "SCHEDULE":                  f"{SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} UTC daily",
        "NOTION_DB_ID":              NOTION_DB_ID,
        "LMTZ_PAGE_ID":              LMTZ_PAGE_ID,
    }


@app.get("/api/node-config")
async def get_node_config():
    h = get_kv("schedule_hour", str(SCHEDULE_HOUR))
    m = get_kv("schedule_minute", str(SCHEDULE_MINUTE))
    return {
        "schedule_time":   f"{int(h):02d}:{int(m):02d}",
        "notion_db_id":    get_kv("notion_db_id",    NOTION_DB_ID),
        "lmtz_page_id":    get_kv("lmtz_page_id",    LMTZ_PAGE_ID),
        "claude_model":    get_kv("claude_model",     "claude-haiku-4-5-20251001"),
        "linkedin_prompt": get_kv("linkedin_prompt",  LINKEDIN_PROMPT),
    }


@app.post("/api/node-config")
async def set_node_config(payload: dict):
    allowed = {"schedule_time", "notion_db_id", "lmtz_page_id", "claude_model", "linkedin_prompt"}
    for key, value in payload.items():
        if key not in allowed:
            continue
        if key == "schedule_time":
            try:
                parts = str(value).split(":")
                h, m = int(parts[0]), int(parts[1])
                set_kv("schedule_hour", str(h))
                set_kv("schedule_minute", str(m))
                if _scheduler and _scheduler.running:
                    _scheduler.reschedule_job("daily_run", trigger="cron", hour=h, minute=m)
            except Exception:
                pass
        else:
            set_kv(key, str(value).strip())
    return {"status": "saved"}


@app.get("/api/brand-knowledge")
async def get_brand_knowledge():
    with get_db() as conn:
        row = conn.execute("SELECT value FROM kv WHERE key='brand_knowledge'").fetchone()
    return {"value": row["value"] if row else ""}


@app.post("/api/brand-knowledge")
async def save_brand_knowledge(payload: dict):
    val = payload.get("value", "")
    with get_db() as conn:
        conn.execute(
            "INSERT INTO kv(key,value) VALUES('brand_knowledge',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (val,)
        )
    return {"status": "saved"}


@app.get("/api/mock-mode")
async def get_mock_mode_api():
    return get_mock_mode()


@app.post("/api/mock-mode")
async def set_mock_mode_api(payload: dict):
    val = json.dumps({"enabled": bool(payload.get("enabled")), "text": payload.get("text", "")})
    with get_db() as conn:
        conn.execute(
            "INSERT INTO kv(key,value) VALUES('mock_mode',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (val,)
        )
    return {"status": "saved"}


@app.post("/api/run")
async def api_run():
    import traceback
    try:
        result = await run_workflow()
        return {"status": "ok", "message": result}
    except Exception as e:
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}


@app.get("/api/review/{review_id}")
async def api_review(review_id: str):
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, post_name, post_content, status FROM pending_reviews WHERE id = ?", (review_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "Not found")
    return dict(row)


@app.get("/review/{review_id}", response_class=HTMLResponse)
async def review_page(review_id: str):
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM pending_reviews WHERE id = ?", (review_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "Review not found")
    if row["status"] != "pending":
        return HTMLResponse(
            f'<html><body style="background:#08080a;color:#d4b896;font-family:Rajdhani,sans-serif;text-align:center;padding:80px">'
            f'<p>This post was already <strong>{row["status"]}</strong>.</p></body></html>'
        )
    return (
        _REVIEW_HTML
        .replace("__CSS__", _KNIGHTS_CSS)
        .replace("__REVIEW_ID__", review_id)
        .replace("__POST_NAME__", row["post_name"] or "")
        .replace("__POST_CONTENT__", row["post_content"] or "")
    )


@app.post("/review/{review_id}/approve", response_class=HTMLResponse)
async def approve_post(review_id: str, request: Request):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    edited = body.get("content", "").strip() or None
    ok, err = await _do_approve(review_id, content=edited)
    if not ok:
        return HTMLResponse(
            f'<html><body style="background:#08080a;color:#e04028;font-family:monospace;padding:40px">'
            f'<b>Error:</b> {err}</body></html>',
            status_code=200,
        )
    return HTMLResponse(
        '<html><body style="background:#08080a;color:#28e060;font-family:Rajdhani,sans-serif;text-align:center;padding:80px">'
        "<h2>✓ Posted to LinkedIn</h2>"
        "<p style='color:#7a6040;margin-top:8px'>Notion page updated to Posted 🎉</p>"
        "</body></html>"
    )


@app.post("/review/{review_id}/reject", response_class=HTMLResponse)
async def reject_post(review_id: str):
    await _do_reject(review_id)
    return HTMLResponse(
        '<html><body style="background:#08080a;color:#e04028;font-family:Rajdhani,sans-serif;text-align:center;padding:80px">'
        "<h2>✗ Post rejected</h2>"
        "</body></html>"
    )


@app.post("/slack/interactive")
async def slack_interactive(request: Request, background_tasks: BackgroundTasks):
    body_bytes = await request.body()

    # Optional signature verification
    if SLACK_SIGNING_SECRET:
        ts = request.headers.get("X-Slack-Request-Timestamp", "")
        sig = request.headers.get("X-Slack-Signature", "")
        base = f"v0:{ts}:{body_bytes.decode()}"
        expected = "v0=" + hmac.new(
            SLACK_SIGNING_SECRET.encode(), base.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise HTTPException(403, "Invalid Slack signature")

    form = await request.form()
    try:
        payload = json.loads(form.get("payload", "{}"))
    except Exception:
        raise HTTPException(400, "Bad payload")

    actions = payload.get("actions", [])
    if not actions:
        return Response("ok")

    action = actions[0]
    action_id = action.get("action_id")
    review_id = action.get("value", "")
    response_url = payload.get("response_url", "")

    if action_id not in ("approve", "reject"):
        return Response("ok")

    async def process():
        if action_id == "approve":
            ok, err = await _do_approve(review_id)
            if ok:
                result = ":white_check_mark: *Approved and posted to LinkedIn!*"
            else:
                result = f":x: Error: {err}"
        else:
            ok, err = await _do_reject(review_id)
            result = ":x: *Post rejected.*" if ok else f":x: Error: {err}"

        if response_url:
            async with httpx.AsyncClient(timeout=15) as client:
                await client.post(
                    response_url,
                    json={
                        "replace_original": True,
                        "text": result,
                        "blocks": [
                            {"type": "section", "text": {"type": "mrkdwn", "text": result}}
                        ],
                    },
                )

    background_tasks.add_task(process)
    return Response("", status_code=200)


@app.get("/debug-notion")
async def debug_notion():
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json={"page_size": 3}, headers=NOTION_HEADERS)
        r.raise_for_status()
    results = r.json().get("results", [])
    out = []
    for page in results:
        props = {}
        for name, prop in page.get("properties", {}).items():
            ptype = prop.get("type")
            val = prop.get(ptype)
            props[name] = {"type": ptype, "raw": val}
        out.append({"id": page["id"], "properties": props})
    return out


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}
