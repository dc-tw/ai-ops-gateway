import argparse
import asyncio
import base64
from pathlib import Path

from app.core.config import get_settings
from app.tools.browser import BrowserSessionManager


def save_screenshot(output: Path, data: bytes) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(data)


async def run(url: str) -> None:
    manager = BrowserSessionManager(get_settings())
    try:
        started = await manager.start(url)
        session_id = str(started["session_id"])
        await manager.type_text(session_id, "input[name='my-text']", "Codex browser session")
        clicked = await manager.click(session_id, "button")
        screenshot = await manager.screenshot(session_id)
        output = Path("output/playwright/browser-session.png")
        await asyncio.to_thread(
            save_screenshot,
            output,
            base64.b64decode(str(screenshot["base64"])),
        )
        print(f"session_id={session_id}")
        print(f"final_url={clicked['url']}")
        print(f"screenshot={output}")
        print(f"closed={await manager.close(session_id)}")
    finally:
        await manager.close_all()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "url",
        nargs="?",
        default="https://www.selenium.dev/selenium/web/web-form.html",
    )
    args = parser.parse_args()
    asyncio.run(run(args.url))


if __name__ == "__main__":
    main()
