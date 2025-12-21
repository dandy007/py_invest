"""
Helpers that let the DB agent retrieve market news via FMP and
download the full text of an article using a headless browser.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Dict, List, Optional

from stocks.data_providers.fmp import FMP, FMPException_LimitReached

# Optional imports for article scraping
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


@dataclass
class NewsQueryConfig:
    ticker: str
    lookback_days: int = 7
    max_results: int = 25
    lang: Optional[str] = None
    region: Optional[str] = None
    query: Optional[str] = None


class TickerNewsService:
    """Fetch ticker-centric news via FinancialModelingPrep."""

    def __init__(self, fmp_client: Optional[FMP] = None):
        self.fmp = fmp_client or FMP()

    def fetch(self, config: NewsQueryConfig) -> List[Dict]:
        lookback = max(1, config.lookback_days or 1)
        limit = max(1, int(config.max_results or 1))
        min_date = datetime.now(timezone.utc).replace(tzinfo=timezone.utc) - timedelta(days=lookback)

        try:
            raw_items = self.fmp.get_stock_news(config.ticker, limit * 3)
        except FMPException_LimitReached:
            raise RuntimeError("FMP news API limit reached. Try again later.")
        if raw_items is None:
            return []

        entries: List[Dict] = []
        for item in raw_items:
            normalized = self._normalize_record(config.ticker, config.query, item)
            if not normalized:
                continue
            published_str = normalized.get("datetime") or normalized.get("published")
            if published_str:
                try:
                    published_dt = datetime.fromisoformat(
                        published_str.replace("Z", "+00:00")
                    )
                except ValueError:
                    published_dt = None
            else:
                published_dt = None

            if published_dt and published_dt < min_date:
                continue

            entries.append(normalized)
            if len(entries) >= limit:
                break

        return entries

    @staticmethod
    def _normalize_record(ticker: str, query: Optional[str], item: Dict) -> Dict:
        if not isinstance(item, dict):
            return {}

        published = item.get("publishedDate") or item.get("date")
        dt_str = None
        if isinstance(published, str):
            try:
                dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt_str = dt.isoformat()
            except ValueError:
                dt_str = published

        summary = (
            item.get("text")
            or item.get("content")
            or item.get("description")
            or item.get("summary")
        )

        return {
            "ticker": ticker.upper(),
            "query": (query or f"{ticker.upper()} stock").strip(),
            "title": item.get("title"),
            "summary": summary,
            "source": item.get("site") or item.get("symbol"),
            "link": item.get("url"),
            "datetime": dt_str,
            "published": dt_str,
            "img": item.get("image"),
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
