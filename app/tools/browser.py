from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
import json
import socket
import time
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse
from uuid import uuid4

from bs4 import BeautifulSoup
from langchain_core.tools import StructuredTool

from app.core.config import Settings

if TYPE_CHECKING:
    from playwright.async_api import Route


class UnsafeURLError(ValueError):
    pass


@dataclass(slots=True)
class ScrapedPage:
    url: str
    title: str
    text: str
    characters: int
    retrieved_at: str
    content_sha256: str


@dataclass(slots=True)
class BrowserSession:
    session_id: str
    context: Any
    page: Any
    lock: asyncio.Lock
    last_used: float


class BrowserScraper:
    """Isolated dynamic-page extractor with SSRF and output-size controls."""

    def __init__(self, settings: Settings) -> None:
        self._headless = settings.playwright_headless
        self._timeout_ms = settings.playwright_timeout_ms
        self._max_chars = settings.playwright_max_content_chars
        self._allowed_domains = set(settings.playwright_allowed_domains)

    async def _validate_url(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise UnsafeURLError("Only absolute HTTP(S) URLs are allowed")

        hostname = parsed.hostname.lower().rstrip(".")
        if self._allowed_domains and not any(
            hostname == domain or hostname.endswith(f".{domain}")
            for domain in self._allowed_domains
        ):
            raise UnsafeURLError(f"Domain is not allow-listed: {hostname}")

        def resolve() -> set[str]:
            return {
                item[4][0]
                for item in socket.getaddrinfo(
                    hostname, parsed.port or 443, type=socket.SOCK_STREAM
                )
            }

        try:
            addresses = await asyncio.to_thread(resolve)
        except socket.gaierror as exc:
            raise UnsafeURLError(f"Cannot resolve host: {hostname}") from exc

        for address in addresses:
            ip = ipaddress.ip_address(address)
            if not ip.is_global:
                raise UnsafeURLError(f"Private or non-global destination is blocked: {hostname}")

    async def _guard_route(self, route: Route) -> None:
        try:
            await self._validate_url(route.request.url)
        except (UnsafeURLError, ValueError):
            await route.abort("blockedbyclient")
            return
        await route.continue_()

    @staticmethod
    def _clean_html(html: str) -> tuple[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        for tag in soup.select(
            "script, style, noscript, svg, canvas, nav, footer, form, iframe, template"
        ):
            tag.decompose()
        root = soup.select_one("main, article, [role='main']") or soup.body or soup
        lines = [" ".join(line.split()) for line in root.get_text("\n").splitlines()]
        text = "\n".join(line for line in lines if line)
        return title, text

    async def scrape(self, url: str) -> dict[str, object]:
        """Render a public dynamic web page and return clean, bounded main-page text."""
        try:
            from playwright.async_api import Browser, async_playwright
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is not installed; install the 'browser' project extra"
            ) from exc

        await self._validate_url(url)
        async with async_playwright() as playwright:
            browser: Browser = await playwright.chromium.launch(
                headless=self._headless,
                args=["--disable-dev-shm-usage"],
            )
            try:
                context = await browser.new_context(
                    java_script_enabled=True,
                    service_workers="block",
                    accept_downloads=False,
                )
                await context.route("**/*", self._guard_route)
                page = await context.new_page()
                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self._timeout_ms,
                )
                if response is None or response.status >= 400:
                    status = response.status if response else "no response"
                    raise RuntimeError(f"Navigation failed with status: {status}")
                try:
                    await page.wait_for_load_state(
                        "networkidle", timeout=min(5_000, self._timeout_ms)
                    )
                except PlaywrightTimeoutError:
                    pass  # Many modern sites keep telemetry sockets open indefinitely.
                final_url = page.url
                await self._validate_url(final_url)
                title, text = self._clean_html(await page.content())
                text = text[: self._max_chars]
                return asdict(
                    ScrapedPage(
                        url=final_url,
                        title=title,
                        text=text,
                        characters=len(text),
                        retrieved_at=datetime.now(UTC).isoformat(),
                        content_sha256=hashlib.sha256(text.encode()).hexdigest(),
                    )
                )
            finally:
                await browser.close()


def build_browser_tool(scraper: BrowserScraper) -> StructuredTool:
    return StructuredTool.from_function(
        coroutine=scraper.scrape,
        name="scrape_dynamic_page",
        description=(
            "Open an allow-listed public HTTP(S) page with Chromium, render JavaScript, "
            "and return cleaned main-content text as JSON."
        ),
    )


def serialize_tool_result(result: dict[str, object]) -> str:
    return json.dumps(result, ensure_ascii=False)


class BrowserSessionManager:
    """Keep isolated Chromium contexts alive across bounded click/type/screenshot calls."""

    def __init__(self, settings: Settings) -> None:
        self._scraper = BrowserScraper(settings)
        self._headless = settings.playwright_headless
        self._timeout_ms = settings.playwright_timeout_ms
        self._max_chars = settings.playwright_max_content_chars
        self._ttl_seconds = settings.browser_session_ttl_seconds
        self._max_sessions = settings.browser_max_sessions
        self._playwright: Any = None
        self._browser: Any = None
        self._sessions: dict[str, BrowserSession] = {}
        self._manager_lock = asyncio.Lock()

    async def _ensure_browser(self) -> None:
        if self._browser is not None:
            return
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is not installed; install the 'browser' project extra"
            ) from exc
        async with self._manager_lock:
            if self._browser is None:
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(
                    headless=self._headless,
                    args=["--disable-dev-shm-usage"],
                )

    async def _cleanup_expired(self) -> None:
        cutoff = time.monotonic() - self._ttl_seconds
        expired = [
            session_id
            for session_id, session in self._sessions.items()
            if session.last_used < cutoff
        ]
        for session_id in expired:
            await self.close(session_id)

    async def _get(self, session_id: str) -> BrowserSession:
        await self._cleanup_expired()
        session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"Unknown or expired browser session: {session_id}")
        session.last_used = time.monotonic()
        return session

    async def _snapshot(self, session: BrowserSession) -> dict[str, object]:
        final_url = session.page.url
        await self._scraper._validate_url(final_url)
        title, text = self._scraper._clean_html(await session.page.content())
        text = text[: self._max_chars]
        return {
            "session_id": session.session_id,
            "url": final_url,
            "title": title,
            "text": text,
            "characters": len(text),
            "retrieved_at": datetime.now(UTC).isoformat(),
            "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
        }

    async def start(self, url: str) -> dict[str, object]:
        await self._cleanup_expired()
        if len(self._sessions) >= self._max_sessions:
            raise RuntimeError("Browser session limit reached")
        await self._scraper._validate_url(url)
        await self._ensure_browser()
        context = await self._browser.new_context(
            java_script_enabled=True,
            service_workers="block",
            accept_downloads=False,
            viewport={"width": 1280, "height": 720},
        )
        await context.route("**/*", self._scraper._guard_route)
        page = await context.new_page()
        session_id = str(uuid4())
        session = BrowserSession(
            session_id=session_id,
            context=context,
            page=page,
            lock=asyncio.Lock(),
            last_used=time.monotonic(),
        )
        self._sessions[session_id] = session
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            if response is None or response.status >= 400:
                status = response.status if response else "no response"
                raise RuntimeError(f"Navigation failed with status: {status}")
            return await self._snapshot(session)
        except Exception:
            await self.close(session_id)
            raise

    async def snapshot(self, session_id: str) -> dict[str, object]:
        session = await self._get(session_id)
        async with session.lock:
            return await self._snapshot(session)

    async def click(self, session_id: str, selector: str) -> dict[str, object]:
        session = await self._get(session_id)
        async with session.lock:
            await session.page.locator(selector).first.click(timeout=self._timeout_ms)
            await session.page.wait_for_timeout(250)
            return await self._snapshot(session)

    async def type_text(
        self,
        session_id: str,
        selector: str,
        text: str,
        *,
        clear: bool = True,
    ) -> dict[str, object]:
        if len(text) > 10_000:
            raise ValueError("Text input is too large")
        session = await self._get(session_id)
        async with session.lock:
            locator = session.page.locator(selector).first
            if clear:
                await locator.fill(text, timeout=self._timeout_ms)
            else:
                await locator.press_sequentially(text, timeout=self._timeout_ms)
            return await self._snapshot(session)

    async def screenshot(self, session_id: str, *, full_page: bool = False) -> dict[str, object]:
        session = await self._get(session_id)
        async with session.lock:
            await self._scraper._validate_url(session.page.url)
            image = await session.page.screenshot(type="png", full_page=full_page)
            if len(image) > 10_000_000:
                raise RuntimeError("Screenshot exceeds the 10 MB response limit")
            return {
                "session_id": session_id,
                "url": session.page.url,
                "mime_type": "image/png",
                "base64": base64.b64encode(image).decode("ascii"),
                "bytes": len(image),
            }

    async def close(self, session_id: str) -> bool:
        session = self._sessions.pop(session_id, None)
        if session is None:
            return False
        with suppress(Exception):
            await session.context.close()
        return True

    async def close_all(self) -> None:
        for session_id in list(self._sessions):
            await self.close(session_id)
        if self._browser is not None:
            with suppress(Exception):
                await self._browser.close()
        if self._playwright is not None:
            with suppress(Exception):
                await self._playwright.stop()
        self._browser = None
        self._playwright = None
