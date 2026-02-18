from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Set, Tuple, Union
from urllib.parse import parse_qs, unquote_plus, urlparse

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class KeywordAttribution:
    """
    Key identifying a (search engine, keyword) pair.
    """

    engine_domain: str
    keyword: str


@dataclass
class AnalyzerCounters:
    """Aggregated counters used for logging, monitoring, and validating each run."""

    rows_seen: int = 0
    search_referrers_seen: int = 0
    purchases_seen: int = 0
    purchases_attributed: int = 0
    revenue_attributed: float = 0.0
    bad_revenue_values: int = 0
    purchases_missing_prior_search: int = 0


class ExternalSearchRevenueAnalyzer:
    """
    Streaming analyzer for external search keyword revenue.

      * Visitor identity = ip + user_agent.
      * Last touch attribution: a purchase is credited to the most recent
        external search for that visitor.
      * Internal hosts are excluded from search.
      * Purchase = event_list contains '1'.
      * Revenue is summed from the 4th semicolon field in product_list
        for each product line item.
    """

    # Base domain mapping, subdomains are handled via suffix matching.
    _KEY_PARAM_BY_BASE_DOMAIN: Dict[str, str] = {
        "yahoo.com": "p",
        "google.com": "q",
        "bing.com": "q",
        "msn.com": "q",
    }

    # If mapping misses, try these common query keys in order.
    _COMMON_KEY_CANDIDATES: Tuple[str, ...] = ("q", "p")

    # Minimal hints for hosts that are likely search engines.
    _SEARCH_DOMAIN_HINTS: Tuple[str, ...] = ("google.", "bing.", "yahoo.", "msn.")

    def __init__(self, internal_hosts: Optional[Set[str]] = None) -> None:
        # Internal hosts , defaults to the sample domain from the dataset.
        base_hosts = internal_hosts or {"esshopzilla.com"}
        self._internal_hosts: Set[str] = {h.lower() for h in base_hosts}

        # Visitor key -> last seen external search attribution.
        self._last_touch: Dict[str, KeywordAttribution] = {}

        # (engine, keyword) -> total revenue.
        self._totals: Dict[KeywordAttribution, float] = {}

        self.stats = AnalyzerCounters()

    # ------------------------------------------------------------------
    # Public APIs used by main.py / handler.py
    # ------------------------------------------------------------------

    def run(self, tsv_path: Union[str, Path]) -> Dict[KeywordAttribution, float]:
        """
        Process a tab-separated hitdata file and aggregate revenue.

        This is the path used by:
          * Local CLI:  python -m external_search_revenue.main <input_tsv>
        """
        path = Path(tsv_path)
        log.info("Starting keyword attribution run from file. input=%s", path)

        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            self._assert_schema(reader.fieldnames)
            self._process_rows(reader)

        self._log_summary()
        return dict(self._totals)

    def run_rows(
        self, rows: Iterable[Mapping[str, str]]
    ) -> Dict[KeywordAttribution, float]:
        """
        Streaming-friendly : analyze an iterable of row dicts.

        Used by the AWS Lambda handler, where we stream TSV rows directly
        from S3 instead of reading from a local file.

        Each row must behave like a dict with at least:
        - ip
        - user_agent
        - referrer
        - event_list
        - product_list
        """
        fieldnames = getattr(rows, "fieldnames", None)
        self._assert_schema(fieldnames)

        log.info("Starting keyword attribution run from streamed rows.")
        self._process_rows(rows)
        self._log_summary()
        return dict(self._totals)

    def ranked_rows(
        self, totals: Dict[KeywordAttribution, float]
    ) -> list[tuple[KeywordAttribution, float]]:
        """Sort report rows by revenue descending."""
        return sorted(totals.items(), key=lambda kv: kv[1], reverse=True)

    @staticmethod
    def default_output_filename(tz: str = "America/Chicago") -> str:
        """
        Provide a date based output filename.
        Example:
            2026-02-15_SearchKeywordPerformance.tab
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo

        today = datetime.now(ZoneInfo(tz)).date().isoformat()
        return f"{today}_SearchKeywordPerformance.tab"

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def _process_rows(self, rows: Iterable[Mapping[str, str]]) -> None:
        """
        Shared core logic: iterate over row dicts and populate self._totals
        and self.stats.

        This is used by both `run` (file path) and `run_rows` (streamed rows).
        """
        for row in rows:
            self.stats.rows_seen += 1
            visitor = self._visitor_key(row)

            # 1) If this row has an external search referrer, update last touch.
            referrer = (row.get("referrer") or "").strip()
            if referrer:
                hit = self._extract_search_keyword(referrer)
                if hit is not None:
                    self._last_touch[visitor] = hit
                    self.stats.search_referrers_seen += 1

            # 2) Only continue for purchase events.
            event_list = (row.get("event_list") or "").strip()
            if not self._is_purchase(event_list):
                continue

            self.stats.purchases_seen += 1

            revenue = self._purchase_revenue((row.get("product_list") or "").strip())
            if revenue <= 0.0:
                continue

            last = self._last_touch.get(visitor)
            if last is None:
                # No prior external search for this visitor.
                self.stats.purchases_missing_prior_search += 1
                continue

            current = self._totals.get(last, 0.0)
            self._totals[last] = current + revenue
            self.stats.purchases_attributed += 1
            self.stats.revenue_attributed += revenue

    def _log_summary(self) -> None:
        """Emit a one-line summary for observability in logs."""
        log.info(
            (
                "Completed keyword attribution run. "
                "rows_seen=%d search_referrers_seen=%d purchases_seen=%d "
                "purchases_attributed=%d revenue_attributed=%.2f "
                "bad_revenue_values=%d purchases_missing_prior_search=%d"
            ),
            self.stats.rows_seen,
            self.stats.search_referrers_seen,
            self.stats.purchases_seen,
            self.stats.purchases_attributed,
            self.stats.revenue_attributed,
            self.stats.bad_revenue_values,
            self.stats.purchases_missing_prior_search,
        )

    # ------------------------
    # Internals / parsing bits
    # ------------------------

    def _assert_schema(self, fieldnames: Optional[list[str]]) -> None:
        needed = {"ip", "user_agent", "referrer", "event_list", "product_list"}
        present = set(fieldnames or [])
        missing = needed - present
        if missing:
            raise ValueError(f"Input TSV missing required columns: {sorted(missing)}")

    def _visitor_key(self, row: Mapping[str, str]) -> str:
        ip = (row.get("ip") or "").strip()
        ua = (row.get("user_agent") or "").strip()
        return f"{ip}|{ua}"

    def _is_purchase(self, event_list: str) -> bool:
        """Purchase if event '1' appears in event_list."""
        if not event_list:
            return False
        return any(tok.strip() == "1" for tok in event_list.split(","))

    def _purchase_revenue(self, product_list: str) -> float:
        """
        Sum revenue from product_list.

        Each product is comma separated; each item has semicolon separated fields:
          category ; product ; quantity ; revenue ;

        We treat the 4th field (index 3) as the revenue and sum it.
        """
        if not product_list:
            return 0.0

        total = 0.0
        for item in (p.strip() for p in product_list.split(",") if p.strip()):
            parts = item.split(";")
            if len(parts) < 4:
                continue

            raw_rev = parts[3].strip()
            if not raw_rev:
                continue

            try:
                total += float(raw_rev)
            except ValueError:
                self.stats.bad_revenue_values += 1

        return total

    def _extract_search_keyword(self, referrer: str) -> Optional[KeywordAttribution]:
        """
        Return KeywordAttribution if referrer looks like an external search engine hit.

        Gates:
          1) URL must parse and have a host
          2) ignore internal hosts
          3) host must look like a search engine
          4) keyword param must exist and yield a non empty keyword
        """
        if not referrer or referrer == "-":
            return None

        try:
            parsed = urlparse(referrer)
        except Exception:
            return None

        host = (parsed.netloc or "").lower().strip()
        if not host:
            return None

        host_norm = self._strip_www(host)

        if self._is_internal_host(host_norm):
            return None

        if not self._looks_like_search_engine(host_norm):
            return None

        query = parse_qs(parsed.query, keep_blank_values=True)
        if not query:
            return None

        key_name = self._pick_keyword_param(host_norm, query)
        if not key_name:
            return None

        vals = query.get(key_name)
        if not vals:
            return None

        keyword = self._normalize_query_value(vals[0])
        if not keyword:
            return None

        base = self._base_domain_match(host_norm)
        engine_domain = base or host_norm

        return KeywordAttribution(engine_domain=engine_domain, keyword=keyword)

    def _strip_www(self, host: str) -> str:
        return host[4:] if host.startswith("www.") else host

    def _is_internal_host(self, host_norm: str) -> bool:
        for internal in self._internal_hosts:
            internal_norm = self._strip_www(internal.lower())
            if host_norm == internal_norm or host_norm.endswith("." + internal_norm):
                return True
        return False

    def _looks_like_search_engine(self, host_norm: str) -> bool:
        # Prefer a proper base domain match if possible.
        if self._base_domain_match(host_norm) is not None:
            return True
        return any(tok in host_norm for tok in self._SEARCH_DOMAIN_HINTS)

    def _base_domain_match(self, host_norm: str) -> Optional[str]:
        for base in self._KEY_PARAM_BY_BASE_DOMAIN.keys():
            if host_norm == base or host_norm.endswith("." + base):
                return base
        return None

    def _pick_keyword_param(self, host_norm: str, query: dict) -> Optional[str]:
        """
        Choose the query parameter that holds the search term.

        1) If host matches a known base domain -> use its mapped param.
        2) Else try a small common set of keys (q, then p).
        """
        base = self._base_domain_match(host_norm)
        if base:
            expected = self._KEY_PARAM_BY_BASE_DOMAIN.get(base)
            if expected and expected in query:
                return expected

        for candidate in self._COMMON_KEY_CANDIDATES:
            if candidate in query:
                return candidate

        return None

    def _normalize_query_value(self, raw: str) -> str:
        """
        Normalize keyword:
          - URL decode
          - convert '+' to spaces
          - collapse whitespace
          - lowercase for consistent grouping
        """
        txt = unquote_plus(raw or "").strip()
        if not txt:
            return ""
        return " ".join(txt.split()).lower()
