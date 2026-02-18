from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional, Sequence

from .analyzer import ExternalSearchRevenueAnalyzer, KeywordAttribution

logger = logging.getLogger(__name__)


def run_etl(input_path: Path, output_dir: Path) -> Path:
    """
    Core ETL entrypoint for local CLI runs.

    It reads the input TSV, runs the ExternalSearchRevenueAnalyzer,
    and writes a tab-separated report into the given output directory.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Ensure output directory exists.
    output_dir.mkdir(parents=True, exist_ok=True)

    analyzer = ExternalSearchRevenueAnalyzer()
    totals = analyzer.run(str(input_path))

    # Use the analyzer's helper to rank rows by revenue descending.
    ranked: list[tuple[KeywordAttribution, float]] = analyzer.ranked_rows(totals)

    # Example filename: 2026-02-15_SearchKeywordPerformance.tab
    output_file = output_dir / analyzer.default_output_filename()

    with output_file.open("w", encoding="utf-8") as f:
        f.write("Search Engine Domain\tSearch Keyword\tRevenue\n")
        for attribution, revenue in ranked:
            f.write(
                f"{attribution.engine_domain}\t{attribution.keyword}\t{revenue:.2f}\n"
            )

    logger.info("Wrote %d rows to %s", len(ranked), output_file)
    return output_file


def cli_main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Command-line entrypoint.

    Requirement (from the PDF):
      python -m external_search_revenue.main <input_tsv>

    * Takes exactly one argument: the path to the input TSV.
    * Writes the output report into the output directory.
    """
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) != 1:
        print(
            "Usage: python -m external_search_revenue.main <input_tsv>",
            file=sys.stderr,
        )
        return 2

    input_path = Path(argv[0])
    output_dir = Path("output")

    try:
        run_etl(input_path, output_dir)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:  # safety net for unexpected errors
        logger.exception("Unexpected error while running ETL")
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    # Simple default logging for local runs.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    raise SystemExit(cli_main())
