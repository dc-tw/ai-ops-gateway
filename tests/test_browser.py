import asyncio
import base64
import time
from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.tools.browser import BrowserScraper, BrowserSession, BrowserSessionManager, UnsafeURLError


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "http://127.0.0.1/admin",
        "http://localhost:8000/healthz",
        "http://169.254.169.254/latest/meta-data/",
    ],
)
async def test_ssrf_targets_are_blocked(url: str) -> None:
    scraper = BrowserScraper(Settings())
    with pytest.raises(UnsafeURLError):
        await scraper._validate_url(url)


def test_clean_html_removes_scripts_navigation_and_noise() -> None:
    title, text = BrowserScraper._clean_html(
        """<html><head><title> Demo </title><script>steal()</script></head>
        <body><nav>menu</nav><main><h1>Hello</h1><p> useful   data </p></main></body></html>"""
    )
    assert title == "Demo"
    assert text == "Hello\nuseful data"
    assert "steal" not in text


class FakeLocator:
    def __init__(self) -> None:
        self.clicked = False
        self.value = ""
        self.first = self

    async def click(self, **kwargs: object) -> None:
        self.clicked = True

    async def fill(self, text: str, **kwargs: object) -> None:
        self.value = text

    async def press_sequentially(self, text: str, **kwargs: object) -> None:
        self.value += text


class FakePage:
    url = "https://example.com/"

    def __init__(self) -> None:
        self.target = FakeLocator()

    def locator(self, selector: str) -> FakeLocator:
        return self.target

    async def wait_for_timeout(self, milliseconds: int) -> None:
        return None

    async def content(self) -> str:
        return "<html><head><title>Example</title></head><body><main>hello</main></body></html>"

    async def screenshot(self, **kwargs: object) -> bytes:
        return b"png-image"


@pytest.mark.asyncio
async def test_persistent_browser_session_click_type_and_screenshot(monkeypatch) -> None:
    manager = BrowserSessionManager(Settings())
    page = FakePage()
    manager._sessions["session-1"] = BrowserSession(
        session_id="session-1",
        context=SimpleNamespace(),
        page=page,
        lock=asyncio.Lock(),
        last_used=time.monotonic(),
    )

    async def allow_url(url: str) -> None:
        return None

    monkeypatch.setattr(manager._scraper, "_validate_url", allow_url)
    clicked = await manager.click("session-1", "button")
    typed = await manager.type_text("session-1", "input", "hello")
    screenshot = await manager.screenshot("session-1")

    assert page.target.clicked is True
    assert page.target.value == "hello"
    assert clicked["session_id"] == typed["session_id"] == "session-1"
    assert base64.b64decode(str(screenshot["base64"])) == b"png-image"
