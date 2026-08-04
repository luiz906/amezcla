import os
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from anthropic import Anthropic
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

# ---------------------------------------------------------------------------
# Config (all via environment variables)
# ---------------------------------------------------------------------------
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = "13ccc694-e848-80ff-8543-fb2b57e1981a"
LMTZ_PAGE_ID = "157cc694-e848-81ad-87aa-000b8ea1d01e"

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

BLOTATO_API_KEY = os.environ["BLOTATO_API_KEY"]
BLOTATO_LINKEDIN_ACCOUNT_ID = os.environ["BLOTATO_LINKEDIN_ACCOUNT_ID"]

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
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
                created_at      TEXT
            )
        """)


# ---------------------------------------------------------------------------
# Notion helpers
# ---------------------------------------------------------------------------
async def find_notion_page() -> dict | None:
    """Return the first Not Started / LMTZ page, or None."""
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {
        "filter": {
            "and": [
                {"property": "Status", "status": {"equals": "Not Started"}},
                {"property": "ClientsOS", "relation": {"contains": LMTZ_PAGE_ID}},
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

Add 3 hashtags, 1 topic, 1 target audience, 1 general
Output only the copy. No section titles, no labels like "Rehook:" "Title:", no formatting notes. DO NOT ADD "POWER STATEMENT:" Don't add #underdogs, replace with #smallbiz.

---
Page properties:
{properties}

Page content:
{content}\
"""


def generate_linkedin_post(post_name: str, properties: str, content: str) -> str:
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    system = "You are a social media content expert and brand designer for Amezcla."
    if BRAND_KNOWLEDGE:
        system += f"\n\nBrand knowledge:\n{BRAND_KNOWLEDGE}"

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=system,
        messages=[
            {
                "role": "user",
                "content": LINKEDIN_PROMPT.format(
                    post_name=post_name,
                    properties=properties,
                    content=content,
                ),
            }
        ],
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
    review_url = f"{BASE_URL}/review/{review_id}"
    preview_short = preview[:400] + ("..." if len(preview) > 400 else "")
    payload = {
        "text": (
            f"*LinkedIn post ready for review* ✏️\n"
            f"*{post_name}*\n\n"
            f"```{preview_short}```\n\n"
            f"<{review_url}|→ Review & Approve>"
        )
    }
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(SLACK_WEBHOOK_URL, json=payload)


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
async def run_workflow():
    print(f"[{datetime.now()}] Running LinkedIn post workflow...")

    page = await find_notion_page()
    if not page:
        print("  No pages found with Status=Not Started and ClientsOS=LMTZ. Skipping.")
        return

    page_id = page["id"]
    post_name = _extract_page_name(page)
    print(f"  Page: {post_name!r} ({page_id})")

    properties = await get_page_properties_as_text(page)
    content = await get_page_blocks_as_text(page_id)

    post_text = generate_linkedin_post(post_name, properties, content)
    print(f"  Generated {len(post_text)} chars of copy.")

    await append_to_notion_page(page_id, post_text)

    review_id = str(uuid.uuid4())
    with get_db() as conn:
        conn.execute(
            "INSERT INTO pending_reviews VALUES (?,?,?,?,?,?)",
            (review_id, page_id, post_name, post_text, "pending",
             datetime.now(timezone.utc).isoformat()),
        )

    await notify_slack(review_id, post_name, post_text)
    print(f"  Review ready: {BASE_URL}/review/{review_id}")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_workflow, "cron", hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE)
    scheduler.start()
    print(f"Scheduler started — runs daily at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} UTC")
    yield
    scheduler.shutdown()


app = FastAPI(title="LinkedIn Post Automation", lifespan=lifespan)

_REVIEW_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Review: {post_name}</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
        max-width:700px;margin:48px auto;padding:0 20px;color:#1a1a1a}}
  h1{{font-size:1.4rem;margin-bottom:4px}}
  .post{{white-space:pre-wrap;background:#f7f7f7;border:1px solid #e0e0e0;
         border-radius:8px;padding:20px;line-height:1.65;font-size:.95rem;
         margin:20px 0}}
  .actions{{display:flex;gap:12px;margin-top:24px}}
  button{{padding:12px 28px;border:none;border-radius:6px;font-size:1rem;
          cursor:pointer;font-weight:600}}
  .approve{{background:#0077b5;color:#fff}}
  .reject {{background:#e74c3c;color:#fff}}
  .done{{padding:60px 0;text-align:center;font-size:1.2rem}}
</style>
</head>
<body>
<h1>LinkedIn Post Review</h1>
<p style="color:#555;margin-top:4px">{post_name}</p>
<div class="post">{post_content}</div>
<div class="actions">
  <form method="POST" action="/review/{review_id}/approve">
    <button class="approve" type="submit">✓ Approve &amp; Post to LinkedIn</button>
  </form>
  <form method="POST" action="/review/{review_id}/reject">
    <button class="reject" type="submit">✗ Reject</button>
  </form>
</div>
</body></html>"""


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
            f'<div class="done" style="font-family:sans-serif;padding:60px;text-align:center">'
            f'This post was already <strong>{row["status"]}</strong>.</div>'
        )
    return _REVIEW_HTML.format(
        review_id=review_id,
        post_name=row["post_name"],
        post_content=row["post_content"],
    )


@app.post("/review/{review_id}/approve", response_class=HTMLResponse)
async def approve_post(review_id: str):
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM pending_reviews WHERE id = ?", (review_id,)
        ).fetchone()
        if not row or row["status"] != "pending":
            raise HTTPException(400, "Not found or already processed")

    await post_to_linkedin_via_blotato(row["post_content"])
    await mark_notion_page_posted(row["notion_page_id"])

    with get_db() as conn:
        conn.execute(
            "UPDATE pending_reviews SET status = 'approved' WHERE id = ?", (review_id,)
        )

    return HTMLResponse(
        '<html><body style="font-family:sans-serif;text-align:center;padding:80px">'
        "<h2>✅ Posted to LinkedIn!</h2>"
        "<p>The Notion page has been updated to <em>Posted 🎉</em>.</p>"
        "</body></html>"
    )


@app.post("/review/{review_id}/reject", response_class=HTMLResponse)
async def reject_post(review_id: str):
    with get_db() as conn:
        conn.execute(
            "UPDATE pending_reviews SET status = 'rejected' WHERE id = ?", (review_id,)
        )
    return HTMLResponse(
        '<html><body style="font-family:sans-serif;text-align:center;padding:80px">'
        "<h2>❌ Post rejected.</h2>"
        "</body></html>"
    )


@app.get("/run-now")
async def run_now():
    """Manual trigger — hit this endpoint to run the workflow immediately."""
    import traceback
    try:
        await run_workflow()
        return {"status": "done"}
    except Exception as e:
        return {"status": "error", "error": str(e), "trace": traceback.format_exc()}


@app.get("/pending")
async def list_pending():
    """List all pending reviews."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, post_name, status, created_at FROM pending_reviews ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}
