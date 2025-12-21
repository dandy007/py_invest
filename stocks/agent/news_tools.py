"""
Helpers that let the DB agent retrieve market news via GoogleNews and
download the full text of an article using a headless browser.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

# Optional imports – the agent should still boot even if dependencies
# were not installed yet.  The actual methods will raise a helpful
# error message when something is missing.
try:
    from GoogleNews import GoogleNews  # type: ignore
except ImportError:  # pragma: no cover - dependency missing at runtime
    GoogleNews = None  # type: ignore

try:
    from playwright.sync_api import (  # type: ignore
        TimeoutError as PlaywrightTimeoutError,
        sync_playwright,
    )
except ImportError:  # pragma: no cover - dependency missing at runtime
    PlaywrightTimeoutError = TimeoutError  # type: ignore
    sync_playwright = None  # type: ignore


class DependencyNotInstalledError(RuntimeError):
    """Raised when an optional dependency is missing."""


def _format_date(dt: datetime) -> str:
    """Format datetimes the way GoogleNews expects (MM/DD/YYYY)."""
    return dt.strftime("%m/%d/%Y")


@dataclass
class NewsQueryConfig:
    ticker: str
    lookback_days: int = 7
    max_results: int = 25
    lang: Optional[str] = None
    region: Optional[str] = None
    query: Optional[str] = None


class TickerNewsService:
    """Wrapper above GoogleNews tailored for ticker centric queries."""

    def __init__(self, default_lang: str = "en", default_region: str = "US"):
        self.default_lang = default_lang
        self.default_region = default_region

    def fetch(self, config: NewsQueryConfig) -> List[Dict]:
        if GoogleNews is None:
            raise DependencyNotInstalledError(
                "GoogleNews package is missing. Install it with 'pip install GoogleNews'."
            )

        lookback = max(1, config.lookback_days or 1)
        query = (config.query or f"{config.ticker.upper()} stock").strip()
        lang = (config.lang or self.default_lang).strip()
        region = (config.region or self.default_region).strip()

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=lookback)

        client = GoogleNews(lang=lang, region=region, encode="utf-8")
        client.clear()
        client.set_time_range(_format_date(start_date), _format_date(end_date))
        client.search(query)

        # Pull enough pages to satisfy max_results (Google usually gives ~10/page)
        desired = max(1, int(config.max_results or 1))
        max_pages = min(10, math.ceil(desired / 10))

        for page in range(1, max_pages + 1):
            try:
                client.get_page(page)
            except Exception:
                break

            if len(client.results()) >= desired:
                break

        entries = []
        for item in client.results()[:desired]:
            entries.append(self._normalize_record(config.ticker, query, item))

        return entries

    @staticmethod
    def _normalize_record(ticker: str, query: str, item: Dict) -> Dict:
        dt_value = item.get("datetime")
        if isinstance(dt_value, datetime):
            dt_str = dt_value.isoformat()
        elif isinstance(dt_value, str):
            dt_str = dt_value
        else:
            dt_str = None

        published = item.get("date")
        if isinstance(published, datetime):
            published = published.isoformat()

        return {
            "ticker": ticker.upper(),
            "query": query,
            "title": item.get("title"),
            "summary": item.get("desc"),
            "source": item.get("media"),
            "link": item.get("link"),
            "datetime": dt_str,
            "published": published,
            "img": item.get("img"),
        }


@dataclass
class ArticleFetchConfig:
    url: str
    wait_selector: Optional[str] = None
    timeout_ms: int = 20000
    max_chars: Optional[int] = None


class ArticleContentFetcher:
    """Fetch article HTML/text using Playwright so JS heavy sites render."""

    def __init__(self, headless: bool = True, wait_until: str = "networkidle"):
        self.headless = headless
        self.wait_until = wait_until

    def fetch(self, config: ArticleFetchConfig) -> Dict:
        if sync_playwright is None:
            raise DependencyNotInstalledError(
                "Playwright is missing. Install it with 'pip install playwright' "
                "and run 'playwright install' once to download browser binaries."
            )

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()

            try:
                page.goto(
                    config.url,
                    wait_until=self.wait_until,
                    timeout=config.timeout_ms or 20000,
                )

                if config.wait_selector:
                    page.wait_for_selector(
                        config.wait_selector, timeout=config.timeout_ms or 20000
                    )

                html = page.content()
                text_content = page.inner_text("body")
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(f"Timeout while loading article: {exc}") from exc
            finally:
                context.close()
                browser.close()

        if config.max_chars:
            html = html[: config.max_chars]
            text_content = text_content[: config.max_chars]

        return {
            "url": config.url,
            "html": html,
            "text": text_content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

