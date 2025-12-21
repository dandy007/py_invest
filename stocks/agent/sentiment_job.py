from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

from dotenv import load_dotenv
from openai import OpenAI


# Make sure the project root is on sys.path
AGENT_DIR = Path(__file__).parent.resolve()
STOCKS_DIR = AGENT_DIR.parent
ROOT_DIR = STOCKS_DIR.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


# Load environment variables (same strategy as the CLI agent)
ENV_LOCATIONS = [
    ROOT_DIR / ".env",
    STOCKS_DIR / ".env",
    AGENT_DIR / ".env",
    Path(".env").resolve(),
]

for env_path in ENV_LOCATIONS:
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        break
else:
    load_dotenv()


# Local imports (after path + env adjustments)
from stocks.agent.config_loader import DEFAULT_SENTIMENT_PROMPT, load_config
from stocks.agent.db_tools import DBTools, TOOLS
from stocks.agent.news_tools import NewsQueryConfig, TickerNewsService
from stocks.db.constants import TICKERS_TIME_DATA__TYPE__CONST
from stocks.db.dao_tickers import DAO_Tickers
from stocks.db.db import DB


logger = logging.getLogger("import_scheduler_logger")


class _SafeDict(dict):
    """Helper that keeps missing format placeholders intact."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class SentimentJob:
    """Scheduled sentiment analysis driver that reuses the DB agent stack."""

    def __init__(self) -> None:
        self.config = load_config()
        self.settings = self.config.get("sentiment_job", {}) or {}
        self.enabled = bool(self.settings.get("enabled"))
        self.lookback_days = int(self.settings.get("lookback_days", 7))
        self.max_articles = int(self.settings.get("max_articles", 12))
        self.max_tickers = max(1, int(self.settings.get("max_tickers_per_run", 5) or 1))
        self.refresh_days = float(self.settings.get("refresh_days", 1) or 0)
        self.min_market_cap = float(
            self.settings.get("min_market_cap", 1_000_000_000) or 1_000_000_000
        )
        self.prompt_template = (
            self.settings.get("prompt") or DEFAULT_SENTIMENT_PROMPT
        )
        self.system_prompt = self.settings.get(
            "system_prompt",
            "Jsi financni analytik, ktery vytvari jednotne sentiment skore 0-100.",
        )
        self.model = (
            self.settings.get("model")
            or self.config.get("model")
            or "google/gemini-2.0-flash-001"
        )

        self.client: Optional[OpenAI] = None
        self.db_tools: Optional[DBTools] = None
        self.conn = None
        self.dao_tickers: Optional[DAO_Tickers] = None
        self.news_service = TickerNewsService()

    # --------------------------------------------------------------------- #
    # Setup helpers
    # --------------------------------------------------------------------- #
    def _ensure_client(self) -> None:
        if self.client:
            return

        api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
        if api_key.startswith('"') and api_key.endswith('"'):
            api_key = api_key[1:-1]
        if api_key.startswith("'") and api_key.endswith("'"):
            api_key = api_key[1:-1]
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY is missing for sentiment job.")

        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
            default_headers={
                "HTTP-Referer": "https://github.com/dandy007/py_invest",
                "X-Title": "PyInvest Sentiment Job",
            },
        )

    def _ensure_db(self) -> None:
        if self.conn and self.dao_tickers and self.db_tools:
            return

        self.conn = DB.get_connection_mysql()
        self.dao_tickers = DAO_Tickers(self.conn)
        self.db_tools = DBTools()

    # --------------------------------------------------------------------- #
    def close(self) -> None:
        if self.db_tools:
            self.db_tools.close()
            self.db_tools = None
        if self.conn:
            self.conn.close()
            self.conn = None
            self.dao_tickers = None

    # --------------------------------------------------------------------- #
    def run(
        self,
        ticker_ids: Optional[Iterable[str]] = None,
        prompt_override: Optional[str] = None,
    ) -> None:
        if not self.enabled:
            logger.info("Sentiment job disabled in config; skipping run.")
            return

        self._ensure_client()
        self._ensure_db()
        assert self.dao_tickers is not None

        if ticker_ids:
            tickers = self._resolve_tickers(ticker_ids)
            if not tickers:
                logger.info("Sentiment job: no manual tickers to analyze.")
                return
            logger.info(
                "Sentiment job: analyzing %s provided tickers.", len(tickers)
            )
            self._process_ticker_batch(tickers, prompt_override)
            return

        total_processed = 0
        batch_number = 0
        skipped: set[str] = set()
        while True:
            tickers = self._resolve_tickers(None, skipped)
            if not tickers:
                if total_processed == 0:
                    logger.info("Sentiment job: no tickers require refresh.")
                else:
                    logger.info(
                        "Sentiment job: completed refresh for %s tickers.",
                        total_processed,
                    )
                return

            batch_number += 1
            logger.info(
                "Sentiment job: batch %s analyzing %s tickers.",
                batch_number,
                len(tickers),
            )
            processed, attempted = self._process_ticker_batch(
                tickers, prompt_override, batch_number=batch_number
            )
            total_processed += processed
            skipped.update(attempted)

    # --------------------------------------------------------------------- #
    def _process_ticker_batch(
        self,
        tickers: List[str],
        prompt_override: Optional[str],
        batch_number: Optional[int] = None,
    ) -> tuple[int, List[str]]:
        if not tickers:
            return 0, []

        processed = 0
        total = len(tickers)
        label_prefix = (
            f"batch {batch_number} - " if batch_number is not None else ""
        )
        attempted: List[str] = []

        for index, ticker_id in enumerate(tickers, start=1):
            logger.info(
                "Sentiment job: %s%s (%s/%s)",
                label_prefix,
                ticker_id,
                index,
                total,
            )
            try:
                result = self._analyze_ticker(ticker_id, prompt_override)
                if not result:
                    continue
                self._persist_sentiment(ticker_id, result["score"])
                logger.info(
                    "Sentiment job: %s%s score=%s rating=%s news=%s",
                    label_prefix,
                    ticker_id,
                    result["score"],
                    result.get("rating"),
                    result.get("news_count"),
                )
                processed += 1
            except Exception as exc:
                logger.error("Sentiment job failed for %s: %s", ticker_id, exc)
            finally:
                attempted.append(ticker_id)

        return processed, attempted

    # --------------------------------------------------------------------- #
    def _resolve_tickers(
        self,
        manual_ids: Optional[Iterable[str]],
        skip_ids: Optional[Iterable[str]] = None,
    ) -> List[str]:
        limit = self.max_tickers
        skip_lookup = {tid.upper() for tid in (skip_ids or []) if tid}
        if manual_ids:
            resolved: List[str] = []
            for ticker in manual_ids:
                if not ticker:
                    continue
                tid = ticker.upper()
                if tid in skip_lookup:
                    continue
                if tid not in resolved:
                    resolved.append(tid)
                if len(resolved) >= limit:
                    break
            return resolved

        assert self.dao_tickers is not None
        cursor = self.dao_tickers.cursor
        cutoff = datetime.utcnow() - timedelta(days=self.refresh_days)

        params: List[object] = []
        where_clauses = ["(sentiment_date IS NULL OR sentiment_date < %s)"]
        params.append(cutoff)

        if self.min_market_cap > 0:
            where_clauses.append("(market_cap IS NOT NULL AND market_cap >= %s)")
            params.append(self.min_market_cap)

        skip_list = sorted(skip_lookup)
        if skip_list:
            placeholders = ", ".join(["%s"] * len(skip_list))
            where_clauses.append(f"ticker_id NOT IN ({placeholders})")
            params.extend(skip_list)

        query = (
            "SELECT ticker_id FROM tickers "
            f"WHERE {' AND '.join(where_clauses)} "
            "ORDER BY sentiment_date IS NULL DESC, sentiment_date ASC "
            "LIMIT %s"
        )
        params.append(limit)

        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        return [row[0].upper() for row in rows]

    # --------------------------------------------------------------------- #
    def _analyze_ticker(
        self, ticker_id: str, prompt_override: Optional[str]
    ) -> Optional[dict]:
        self._ensure_client()
        self._ensure_db()
        assert self.client is not None
        assert self.db_tools is not None

        prompt = self._build_user_prompt(ticker_id, prompt_override)
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": prompt},
        ]

        response_text, news_count, had_news_error = self._chat_with_tools(messages)
        self._log_llm_output(ticker_id, response_text)
        if had_news_error:
            logger.warning(
                "Sentiment job: skipping %s because news fetch failed in tool call.",
                ticker_id,
            )
            return None

        if news_count == 0:
            fallback_news = self._fetch_fmp_news_count(ticker_id)
            logger.warning(
                "Sentiment job: skipping %s because the LLM did not fetch any news (fallback detected %s articles).",
                ticker_id,
                fallback_news,
            )
            return None
        payload = self._parse_json(response_text)

        if not payload:
            raise ValueError("LLM response is not valid JSON.")

        score = self._normalize_score(
            payload.get("sentiment_score")
            or payload.get("score")
            or payload.get("sentiment")
        )
        if score is None:
            raise ValueError("Sentiment score missing in response.")

        payload["score"] = score
        payload["news_count"] = news_count
        return payload

    def _build_user_prompt(
        self, ticker_id: str, prompt_override: Optional[str]
    ) -> str:
        template = prompt_override or self.prompt_template or DEFAULT_SENTIMENT_PROMPT
        context = _SafeDict(
            ticker=ticker_id.upper(),
            lookback_days=self.lookback_days,
            max_articles=self.max_articles,
            today=datetime.utcnow().strftime("%Y-%m-%d"),
        )
        prompt = self._render_prompt_template(template, context)

        extra = (
            f"\n- Pouzij maximalne {self.max_articles} novinek."
            f"\n- Pokud potrebujes dalsi data, vyuzij dostupne nastroje (query_tickers, get_ticker_details, search_ticker_news)."
        )
        return (prompt.strip() + "\n" + extra).strip()

    @staticmethod
    def _render_prompt_template(template: str, context: _SafeDict) -> str:
        if not template:
            return ""
        try:
            return template.format_map(context)
        except Exception:
            rendered = template
            for key, value in context.items():
                rendered = rendered.replace(f"{{{key}}}", str(value))
            return rendered

    def _chat_with_tools(self, messages: List[dict]) -> tuple[str, int, bool]:
        assert self.client is not None
        assert self.db_tools is not None
        news_articles = 0
        news_error = False

        while True:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
            )
            assistant_message = response.choices[0].message

            if assistant_message.tool_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": assistant_message.content,
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }
                            for tc in assistant_message.tool_calls
                        ],
                    }
                )

                for tool_call in assistant_message.tool_calls:
                    func_name = tool_call.function.name
                    try:
                        func_args = json.loads(tool_call.function.arguments or "{}")
                    except json.JSONDecodeError:
                        func_args = {}
                    result = self.db_tools.execute_tool(func_name, func_args)
                    self._log_tool_call(func_name, tool_call.function.arguments or "", result)
                    if func_name == "search_ticker_news":
                        try:
                            parsed = json.loads(result)
                        except Exception:
                            parsed = None
                        if isinstance(parsed, dict):
                            if "error" in parsed:
                                news_error = True
                            elif isinstance(parsed.get("count"), int):
                                news_articles += max(0, parsed["count"])
                            elif isinstance(parsed.get("data"), list):
                                news_articles += len(parsed["data"])
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": result,
                        }
                    )
                continue

            return assistant_message.content or "", news_articles, news_error

    def _fetch_fmp_news_count(self, ticker_id: str) -> int:
        try:
            config = NewsQueryConfig(
                ticker=ticker_id,
                lookback_days=self.lookback_days,
                max_results=self.max_articles,
            )
            records = self.news_service.fetch(config)
            return len(records)
        except Exception as exc:
            logger.warning(
                "Sentiment job: fallback news fetch failed for %s: %s", ticker_id, exc
            )
            return 0

    # --------------------------------------------------------------------- #
    def _log_llm_output(self, ticker_id: str, response_text: str) -> None:
        snippet = self._truncate_text(response_text)
        logger.info("Sentiment job: LLM output for %s: %s", ticker_id, snippet)

    def _log_tool_call(self, name: str, args: str, result: str) -> None:
        logger.info(
            "Sentiment job tool: %s args=%s result=%s",
            name,
            self._truncate_text(args),
            self._truncate_text(result),
        )

    @staticmethod
    def _truncate_text(data: Optional[str], limit: int = 2000) -> str:
        if not data:
            return ""
        data = data.strip()
        if len(data) <= limit:
            return data
        return data[:limit] + "...(truncated)"

    # --------------------------------------------------------------------- #
    @staticmethod
    def _parse_json(text: str) -> Optional[dict]:
        if not text:
            return None

        snippet = text.strip()
        fence = re.search(r"```(?:json)?(.*?)```", snippet, re.DOTALL | re.IGNORECASE)
        if fence:
            snippet = fence.group(1).strip()

        start = snippet.find("{")
        end = snippet.rfind("}")
        if start != -1 and end != -1 and end > start:
            snippet = snippet[start : end + 1]

        try:
            return json.loads(snippet)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _normalize_score(value: Optional[object]) -> Optional[int]:
        if value is None:
            return None
        try:
            score = int(round(float(value)))
        except (TypeError, ValueError):
            return None
        return max(0, min(100, score))

    def _persist_sentiment(self, ticker_id: str, score: int) -> None:
        assert self.dao_tickers is not None
        payload = {
            TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__SENTIMENT: score,
            # Store ISO string to avoid connector issues with DATE columns.
            TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__SENTIMENT_DATE: datetime.utcnow().strftime(
                "%Y-%m-%d"
            ),
        }
        self.dao_tickers.update_ticker_types(ticker_id, payload, True)


def run_sentiment_job(
    ticker_ids: Optional[Iterable[str]] = None,
    prompt_override: Optional[str] = None,
) -> None:
    job = SentimentJob()
    try:
        job.run(ticker_ids=ticker_ids, prompt_override=prompt_override)
    finally:
        job.close()


if __name__ == "__main__":
    run_sentiment_job()
