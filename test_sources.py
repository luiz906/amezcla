"""Tests for multi-source Notion → LinkedIn posting. Notion, Blotato and Slack are mocked.

Run: python -m unittest test_sources -v
"""
import asyncio
import importlib
import os
import sys
import tempfile
import unittest
from unittest import mock

BASE_ENV = {
    "NOTION_TOKEN": "test-notion",
    "ANTHROPIC_API_KEY": "test-anthropic",
    "BLOTATO_API_KEY": "test-blotato",
    "BLOTATO_LINKEDIN_ACCOUNT_ID": "1111",
}
SECOND_ENV = {
    "SOURCE_LABEL_2": "GEFCO",
    "NOTION_DB_ID_2": "772663a4-7b37-820a-aaf2-818b05d7aa3b",
    "NOTION_READY_STATUS_2": "Ready to Post",
    "NOTION_POSTED_STATUS_2": "Posted",
    "BLOTATO_LINKEDIN_ACCOUNT_ID_2": "11255",
    "BLOTATO_LINKEDIN_PAGE_ID_2": "98984707",
}


class FakeResponse:
    def __init__(self, data=None, status_code=200):
        self._data = data if data is not None else {}
        self.status_code = status_code
        self.text = "ok"

    def json(self):
        return self._data

    def raise_for_status(self):
        pass


class FakeAsyncClient:
    """Records every HTTP call and answers Notion queries with a single page."""

    calls: list = []

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def _record(self, method, url, **kw):
        FakeAsyncClient.calls.append({"method": method, "url": url, **kw})
        if url.endswith("/query"):
            db_id = url.split("/databases/")[1].split("/")[0]
            return FakeResponse({"results": [{
                "id": f"page-in-{db_id}",
                "properties": {"Name": {"type": "title", "title": [{"plain_text": f"Post from {db_id}"}]}},
            }]})
        if "/blocks/" in url and method == "GET":
            return FakeResponse({"results": []})
        return FakeResponse({"postSubmissionId": "abc"})

    async def post(self, url, **kw):
        return await self._record("POST", url, **kw)

    async def patch(self, url, **kw):
        return await self._record("PATCH", url, **kw)

    async def get(self, url, **kw):
        return await self._record("GET", url, **kw)


def load_app(extra_env):
    env = {**BASE_ENV, **extra_env, "DB_PATH": tempfile.mktemp(suffix=".db")}
    for k in list(os.environ):
        if k.endswith("_2") or k in ("BLOTATO_LINKEDIN_PAGE_ID", "SOURCE_LABEL"):
            env.setdefault(k, None)
    patched = {k: v for k, v in env.items() if v is not None}
    with mock.patch.dict(os.environ, patched, clear=False):
        for k, v in env.items():
            if v is None:
                os.environ.pop(k, None)
        sys.modules.pop("app", None)
        app = importlib.import_module("app")
    app.init_db()
    app.set_kv("mock_mode", '{"enabled": true, "text": "Mock post copy"}')
    return app


def run(coro):
    return asyncio.run(coro)


class TargetTests(unittest.TestCase):
    def test_target_without_page_id_is_personal_profile(self):
        app = load_app({})
        self.assertEqual(app.build_linkedin_target(""), {"targetType": "linkedin"})

    def test_target_with_page_id(self):
        app = load_app({})
        self.assertEqual(app.build_linkedin_target("98984707"),
                         {"targetType": "linkedin", "pageId": "98984707"})


class SourceConfigTests(unittest.TestCase):
    def test_only_primary_when_second_db_unset(self):
        app = load_app({})
        self.assertEqual(list(app.SOURCES), ["primary"])

    def test_second_source_from_env(self):
        app = load_app(SECOND_ENV)
        src = app.SOURCES["secondary"]
        self.assertEqual(src["label"], "GEFCO")
        self.assertEqual(src["notion_db_id"], SECOND_ENV["NOTION_DB_ID_2"])
        self.assertEqual(src["blotato_account_id"], "11255")
        self.assertEqual(src["linkedin_page_id"], "98984707")


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        FakeAsyncClient.calls = []
        self.app = load_app(SECOND_ENV)
        self.patch = mock.patch.object(self.app.httpx, "AsyncClient", FakeAsyncClient)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def queries(self):
        return [c for c in FakeAsyncClient.calls if c["url"].endswith("/query")]

    def test_runs_both_sources_with_correct_filters(self):
        msg = run(self.app.run_workflow())
        self.assertIn("[Primary]", msg)
        self.assertIn("[GEFCO]", msg)

        q = {c["url"].split("/databases/")[1].split("/")[0]: c["json"] for c in self.queries()}
        primary = q[self.app.NOTION_DB_ID]["filter"]["and"]
        self.assertEqual(primary[0], {"property": "Status", "status": {"equals": "Not Started"}})
        self.assertEqual(primary[1]["property"], "ClientsOS")

        second = q[SECOND_ENV["NOTION_DB_ID_2"]]["filter"]
        self.assertEqual(second, {"property": "Status", "status": {"equals": "Ready to Post"}})

        with self.app.get_db() as conn:
            rows = {r["source"]: r for r in conn.execute("SELECT * FROM pending_reviews")}
        self.assertEqual(set(rows), {"primary", "secondary"})
        self.assertEqual(rows["secondary"]["notion_page_id"],
                         f"page-in-{SECOND_ENV['NOTION_DB_ID_2']}")

    def test_single_source_run(self):
        msg = run(self.app.run_workflow("secondary"))
        self.assertNotIn("[Primary]", msg)
        self.assertEqual(len(self.queries()), 1)

    def _approve(self, source_key):
        run(self.app.run_workflow(source_key))
        with self.app.get_db() as conn:
            review_id = conn.execute("SELECT id FROM pending_reviews WHERE source=?",
                                     (source_key,)).fetchone()["id"]
        FakeAsyncClient.calls = []
        ok, err = run(self.app._do_approve(review_id))
        self.assertTrue(ok, err)
        blotato = [c for c in FakeAsyncClient.calls if "blotato" in c["url"]]
        notion = [c for c in FakeAsyncClient.calls if "/pages/" in c["url"]]
        self.assertEqual(len(blotato), 1)
        self.assertEqual(len(notion), 1)
        return blotato[0]["json"]["post"], notion[0]["json"]["properties"]

    def test_approve_second_source_posts_to_company_page(self):
        post, props = self._approve("secondary")
        self.assertEqual(post["accountId"], "11255")
        self.assertEqual(post["target"], {"targetType": "linkedin", "pageId": "98984707"})
        self.assertEqual(props, {"Status": {"status": {"name": "Posted"}}})

    def test_approve_primary_unchanged(self):
        post, props = self._approve("primary")
        self.assertEqual(post["accountId"], "1111")
        self.assertEqual(post["target"], {"targetType": "linkedin"})
        self.assertEqual(props["Status"], {"status": {"name": "Posted 🎉"}})
        self.assertIn("Posting Date", props)
        self.assertEqual(props["Type"], {"select": {"name": "Written Post"}})

    def test_skip_second_source_marks_posted_in_its_db(self):
        run(self.app.run_workflow("secondary"))
        with self.app.get_db() as conn:
            review_id = conn.execute("SELECT id FROM pending_reviews").fetchone()["id"]
        FakeAsyncClient.calls = []
        ok, err = run(self.app._do_skip(review_id))
        self.assertTrue(ok, err)
        self.assertEqual(err, "")
        self.assertFalse([c for c in FakeAsyncClient.calls if "blotato" in c["url"]])

    def test_second_source_skipped_without_status_names(self):
        self.app.SOURCES["secondary"]["ready_status"] = ""
        msg = run(self.app.run_workflow("secondary"))
        self.assertIn("skipped", msg)
        self.assertEqual(self.queries(), [])

    def test_one_source_failing_does_not_block_other(self):
        orig = self.app.find_notion_page

        async def flaky(src):
            if src["key"] == "primary":
                raise RuntimeError("notion down")
            return await orig(src)

        with mock.patch.object(self.app, "find_notion_page", flaky):
            msg = run(self.app.run_workflow())
        self.assertIn("[Primary] error: notion down", msg)
        self.assertIn("[GEFCO] Post generated", msg)


class MigrationTests(unittest.TestCase):
    def test_existing_rows_default_to_primary(self):
        import sqlite3
        path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE pending_reviews (id TEXT PRIMARY KEY, notion_page_id TEXT NOT NULL,"
                     " post_name TEXT, post_content TEXT, status TEXT DEFAULT 'pending', created_at TEXT)")
        conn.execute("INSERT INTO pending_reviews (id, notion_page_id) VALUES ('old', 'p1')")
        conn.commit()
        conn.close()
        app = load_app({})
        app.DB_PATH = path
        app.init_db()
        with app.get_db() as c:
            self.assertEqual(c.execute("SELECT source FROM pending_reviews").fetchone()["source"], "primary")


if __name__ == "__main__":
    unittest.main()
