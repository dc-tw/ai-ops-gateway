#!/usr/bin/env python3
"""Run the real BrowserScraper against a public page and validate clean text output."""

import argparse
import asyncio
import json
import time

from app.core.config import Settings
from app.tools.browser import BrowserScraper

BLOCK_MARKERS = ("cf-chl-", "cloudflare ray id", "captcha", "access denied")


async def run(url: str) -> int:
    scraper = BrowserScraper(Settings(playwright_headless=True))
    started = time.perf_counter()
    result = await scraper.scrape(url)
    elapsed_ms = (time.perf_counter() - started) * 1_000
    text = str(result["text"])
    lowered = text.lower()

    if len(text) < 40:
        raise AssertionError(f"Scraped text is unexpectedly short: {len(text)} characters")
    if "<script" in lowered or "<html" in lowered:
        raise AssertionError("Scraper returned raw HTML instead of cleaned text")
    found_markers = [marker for marker in BLOCK_MARKERS if marker in lowered]
    if found_markers:
        raise AssertionError(f"Possible anti-bot challenge detected: {found_markers}")

    print(
        json.dumps(
            {
                "status": "passed",
                "url": result["url"],
                "title": result["title"],
                "characters": result["characters"],
                "elapsed_ms": round(elapsed_ms, 1),
                "preview": text[:240],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("url", nargs="?", default="https://example.com")
    args = parser.parse_args()
    return asyncio.run(run(args.url))


if __name__ == "__main__":
    raise SystemExit(main())
