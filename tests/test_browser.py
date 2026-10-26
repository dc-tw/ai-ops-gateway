import asyncio
import base64
import socket
import time
from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.tools.browser import BrowserScraper, BrowserSession, BrowserSessionManager, UnsafeURLError
from app.tools.registry import LocalBrowserToolExecutor


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


class FakeResponse:
    def __init__(self, status=200):
        self.status = status
        self.disposed = False

    async def dispose(self):
        self.disposed = True


class FakeRoute:
    def __init__(self, url, status=200):
        self.request = SimpleNamespace(url=url)
        self.response = FakeResponse(status)
        self.continued = False
        self.aborted = False
        self.fulfilled = False
        self.fetch_options = None

    async def abort(self, reason):
        self.aborted = True

    async def continue_(self):
        self.continued = True

    async def fetch(self, **kwargs):
        self.fetch_options = kwargs
        return self.response

    async def fulfill(self, **kwargs):
        self.fulfilled = True


@pytest.fixture
def public_dns(monkeypatch):
    def resolve(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)


async def test_delegated_domain_is_exact_and_cannot_expand_global_allowlist(public_dns):
    base = BrowserScraper(Settings(playwright_allowed_domains=["example.com"]))
    scoped = base._scoped(["example.com"])
    await scoped._validate_url("https://example.com/page")
    with pytest.raises(UnsafeURLError, match="delegated"):
        await scoped._validate_url("https://sub.example.com/")
    with pytest.raises(UnsafeURLError, match="allow-listed"):
        await base._scoped(["outside.test"])._validate_url("https://outside.test/")
    # A narrower job must not mutate the base/global browser guard.
    await base._validate_url("https://sub.example.com/")


async def test_delegated_scope_preserves_ssrf_protection():
    scoped = BrowserScraper(Settings())._scoped(["127.0.0.1"])
    with pytest.raises(UnsafeURLError, match="Private"):
        await scoped._validate_url("http://127.0.0.1/private")


async def test_scoped_requests_block_cross_domain_before_any_network_fetch(public_dns):
    scoped = BrowserScraper(Settings())._scoped(["example.com"])
    blocked = FakeRoute("https://outside.test/exfiltrate")
    await scoped._guard_route(blocked)
    assert blocked.aborted is True
    assert blocked.continued is False
    assert blocked.fetch_options is None
    assert blocked.fulfilled is False


@pytest.mark.parametrize("status", [301, 302, 307, 308])
async def test_scoped_http_redirect_is_never_forwarded_to_browser(public_dns, status):
    scoped = BrowserScraper(Settings())._scoped(["example.com"])
    redirect = FakeRoute("https://example.com/redirect", status=status)
    await scoped._guard_route(redirect)
    assert redirect.fetch_options["max_redirects"] == 0
    assert redirect.aborted is True
    assert redirect.fulfilled is False
    assert redirect.continued is False
    assert redirect.response.disposed is True


async def test_scoped_allowed_response_is_fulfilled_and_default_route_is_unchanged(public_dns):
    base = BrowserScraper(Settings())
    scoped = base._scoped(["example.com"])
    allowed = FakeRoute("https://example.com/page")
    await scoped._guard_route(allowed)
    assert allowed.fulfilled is True
    assert allowed.aborted is False
    assert allowed.continued is False
    original = FakeRoute("https://other-public.test/")
    await base._guard_route(original)
    assert original.continued is True
    assert original.fetch_options is None


class ScopedContext:
    def __init__(self):
        self.page = FakePage()
        self.http_handler = None
        self.websocket_handler = None

    async def route(self, pattern, handler):
        self.http_handler = handler

    async def route_web_socket(self, pattern, handler):
        self.websocket_handler = handler

    async def new_page(self):
        async def goto(url, **kwargs):
            self.page.url = url
            return SimpleNamespace(status=200)

        self.page.goto = goto
        return self.page

    async def close(self):
        pass


async def test_persistent_session_retains_network_scope_across_later_actions(public_dns):
    manager = BrowserSessionManager(Settings())
    context = ScopedContext()

    async def new_context(**kwargs):
        return context

    manager._browser = SimpleNamespace(new_context=new_context)
    result = await manager.start("https://example.com/", allowed_domains=["example.com"])
    session = manager._sessions[result["session_id"]]
    assert session.guard._delegated_domains == {"example.com"}
    await manager.click(result["session_id"], "button")
    await manager.type_text(result["session_id"], "input", "test")
    blocked = FakeRoute("https://outside.test/after-human-approval")
    await context.http_handler(blocked)
    assert blocked.aborted is True
    assert blocked.fetch_options is None
    assert context.websocket_handler is not None
    context.page.url = "https://outside.test/"
    with pytest.raises(UnsafeURLError, match="delegated"):
        await manager.snapshot(result["session_id"])
    with pytest.raises(UnsafeURLError, match="delegated"):
        await manager.screenshot(result["session_id"])


async def test_scoped_context_closes_websocket_without_connecting(public_dns):
    scoped = BrowserScraper(Settings())._scoped(["example.com"])
    context = ScopedContext()
    await scoped._install_routes(context)
    called = []

    async def close(**kwargs):
        called.append(kwargs)

    await context.websocket_handler(SimpleNamespace(close=close))
    assert called[0]["code"] == 1008


async def test_local_executor_passes_trusted_domain_scope_to_scrape_and_session():
    captures = []

    async def capture(url, **kwargs):
        captures.append((url, kwargs))
        return {"url": url}

    executor = LocalBrowserToolExecutor(
        SimpleNamespace(scrape=capture), SimpleNamespace(start=capture)
    )
    for tool in ("scrape_dynamic_page", "browser_start"):
        await executor.invoke(
            tool, {"url": "https://example.com/", "allowed_domains": ["example.com"]}
        )
    assert all(kwargs == {"allowed_domains": ["example.com"]} for _, kwargs in captures)
