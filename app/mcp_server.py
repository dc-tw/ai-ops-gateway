from mcp.server.fastmcp import FastMCP

from app.core.config import get_settings
from app.tools.browser import BrowserScraper, BrowserSessionManager
from app.tools.registry import LocalBrowserToolExecutor, RemoteBrowserToolExecutor

mcp = FastMCP("ai-ops-browser-tools")
settings = get_settings()
scraper = BrowserScraper(settings)
sessions = BrowserSessionManager(settings)
executor = (
    RemoteBrowserToolExecutor(
        settings.browser_worker_url,
        settings.browser_worker_token,
        timeout_seconds=max(60.0, settings.playwright_timeout_ms / 1_000 + 10),
    )
    if settings.browser_worker_url
    else LocalBrowserToolExecutor(scraper, sessions)
)


@mcp.tool()
async def scrape_dynamic_page(url: str) -> dict[str, object]:
    """Render an allow-listed public webpage and return cleaned, bounded main content."""
    return await executor.invoke("scrape_dynamic_page", {"url": url})


@mcp.tool()
async def browser_start(url: str) -> dict[str, object]:
    """Start an isolated browser session and return its id plus a cleaned page snapshot."""
    return await executor.invoke("browser_start", {"url": url})


@mcp.tool()
async def browser_click(session_id: str, selector: str) -> dict[str, object]:
    """Click the first matching CSS/text selector in an existing browser session."""
    return await executor.invoke(
        "browser_click", {"session_id": session_id, "selector": selector}
    )


@mcp.tool()
async def browser_type(
    session_id: str,
    selector: str,
    text: str,
    clear: bool = True,
) -> dict[str, object]:
    """Fill or append text in the first matching element of an existing session."""
    return await executor.invoke(
        "browser_type",
        {"session_id": session_id, "selector": selector, "text": text, "clear": clear},
    )


@mcp.tool()
async def browser_screenshot(session_id: str, full_page: bool = False) -> dict[str, object]:
    """Return a PNG screenshot as base64 from an existing browser session."""
    return await executor.invoke(
        "browser_screenshot", {"session_id": session_id, "full_page": full_page}
    )


@mcp.tool()
async def browser_close(session_id: str) -> dict[str, object]:
    """Close a browser session and release its context."""
    return await executor.invoke("browser_close", {"session_id": session_id})


if __name__ == "__main__":
    mcp.run(transport="stdio")
