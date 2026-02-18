import csv
from pathlib import Path

import pytest

from external_search_revenue.analyzer import ExternalSearchRevenueAnalyzer


def write_mini_tsv(path: Path, rows: list[dict]) -> None:
    """
    Write a tiny TSV with only the columns our analyzer requires.
    This keeps tests focused and easy to read.
    """
    columns = ["ip", "user_agent", "referrer", "event_list", "product_list"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in columns})


def totals_as_tuples(totals) -> dict[tuple[str, str], float]:
    """
    Make assertions easier: convert totals to {(domain, keyword): revenue}.
    """
    return {(k.engine_domain, k.keyword): v for k, v in totals.items()}


def test_keyword_is_case_normalized(tmp_path: Path) -> None:
    """Keywords are normalized so 'Ipod' and 'ipod' roll up consistently."""
    input_file = tmp_path / "sample.tsv"

    sample_rows = [
        {
            "ip": "10.0.0.1",
            "user_agent": "UA-Desktop",
            "referrer": "https://www.google.com/search?q=Ipod",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "10.0.0.1",
            "user_agent": "UA-Desktop",
            "referrer": "",
            "event_list": "1",
            "product_list": "Electronics;Ipod;1;290;",
        },
    ]

    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))

    by_key = totals_as_tuples(totals)
    assert ("google.com", "ipod") in by_key
    assert by_key[("google.com", "ipod")] == pytest.approx(290.0)


def test_purchase_uses_most_recent_search_for_same_visitor(tmp_path: Path) -> None:
    """
    Last touch rule: for the same visitor (ip + user_agent), attribute purchase to
    the most recent external search seen before the purchase.
    """
    input_file = tmp_path / "sample.tsv"

    visitor_ip = "10.0.0.2"
    visitor_ua = "UA-Mobile"

    sample_rows = [
        {
            "ip": visitor_ip,
            "user_agent": visitor_ua,
            "referrer": "https://www.google.com/search?q=ipod",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": visitor_ip,
            "user_agent": visitor_ua,
            "referrer": "https://www.bing.com/search?q=zune",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": visitor_ip,
            "user_agent": visitor_ua,
            "referrer": "",
            "event_list": "1",
            "product_list": "Electronics;Zune;1;250;",
        },
    ]

    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))

    by_key = totals_as_tuples(totals)
    assert ("bing.com", "zune") in by_key
    assert by_key[("bing.com", "zune")] == pytest.approx(250.0)


def test_internal_referrer_does_not_count_as_external_search(tmp_path: Path) -> None:
    """Internal referrers are ignored so we don't mis-attribute revenue."""
    input_file = tmp_path / "sample.tsv"

    sample_rows = [
        {
            "ip": "10.0.0.3",
            "user_agent": "UA-Test",
            "referrer": "https://www.esshopzilla.com/search/?k=ipod",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "10.0.0.3",
            "user_agent": "UA-Test",
            "referrer": "",
            "event_list": "1",
            "product_list": "Electronics;Ipod;1;190;",
        },
    ]

    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))

    assert totals == {}
    assert analyzer.stats.purchases_seen == 1
    assert analyzer.stats.purchases_attributed == 0


def test_yahoo_search_uses_p_parameter(tmp_path: Path) -> None:
    """Yahoo uses ?p= (not ?q=) for search queries."""
    input_file = tmp_path / "sample.tsv"
    sample_rows = [
        {
            "ip": "10.0.0.1",
            "user_agent": "UA",
            "referrer": "http://search.yahoo.com/search?p=cd+player&toggle=1",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "10.0.0.1",
            "user_agent": "UA",
            "referrer": "",
            "event_list": "1",
            "product_list": "Electronics;CD Player;1;190;",
        },
    ]
    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))
    by_key = totals_as_tuples(totals)

    assert ("yahoo.com", "cd player") in by_key
    assert by_key[("yahoo.com", "cd player")] == pytest.approx(190.0)


def test_multiple_products_in_single_purchase(tmp_path: Path) -> None:
    """Purchase event can have multiple products, revenue should sum."""
    input_file = tmp_path / "sample.tsv"
    sample_rows = [
        {
            "ip": "10.0.0.1",
            "user_agent": "UA",
            "referrer": "https://www.google.com/search?q=electronics",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "10.0.0.1",
            "user_agent": "UA",
            "referrer": "",
            "event_list": "1",
            "product_list": "Electronics;Ipod;1;290;,Electronics;Case;1;10;",
        },
    ]
    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))
    by_key = totals_as_tuples(totals)

    assert ("google.com", "electronics") in by_key
    assert by_key[("google.com", "electronics")] == pytest.approx(300.0)


def test_non_purchase_events_ignored(tmp_path: Path) -> None:
    """Event 2 (product view) should not count as purchase."""
    input_file = tmp_path / "sample.tsv"
    sample_rows = [
        {
            "ip": "10.0.0.1",
            "user_agent": "UA",
            "referrer": "https://www.google.com/search?q=ipod",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "10.0.0.1",
            "user_agent": "UA",
            "referrer": "",
            "event_list": "2",  # Product view, not purchase
            "product_list": "Electronics;Ipod;1;290;",
        },
    ]
    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))

    assert totals == {}


def test_ranking_sorts_by_revenue_descending(tmp_path: Path) -> None:
    """ranked_rows should sort by revenue in descending order."""
    input_file = tmp_path / "sample.tsv"
    sample_rows = [
        {
            "ip": "1",
            "user_agent": "UA",
            "referrer": "https://google.com/search?q=cheap",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "1",
            "user_agent": "UA",
            "referrer": "",
            "event_list": "1",
            "product_list": "A;B;1;50;",
        },
        {
            "ip": "2",
            "user_agent": "UA",
            "referrer": "https://bing.com/search?q=expensive",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "2",
            "user_agent": "UA",
            "referrer": "",
            "event_list": "1",
            "product_list": "A;B;1;300;",
        },
        {
            "ip": "3",
            "user_agent": "UA",
            "referrer": "https://yahoo.com/search?p=medium",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "3",
            "user_agent": "UA",
            "referrer": "",
            "event_list": "1",
            "product_list": "A;B;1;150;",
        },
    ]
    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))
    ranked = analyzer.ranked_rows(totals)

    assert len(ranked) == 3
    assert ranked[0][1] == pytest.approx(300.0)
    assert ranked[1][1] == pytest.approx(150.0)
    assert ranked[2][1] == pytest.approx(50.0)


def test_purchase_without_prior_search_is_not_attributed(tmp_path: Path) -> None:
    """
    If a purchase happens without any prior external search referrer for that visitor,
    it should not be attributed, and purchases_missing_prior_search should increment.
    """
    input_file = tmp_path / "sample.tsv"

    sample_rows = [
        {
            "ip": "10.0.0.9",
            "user_agent": "UA-NoSearch",
            "referrer": "",
            "event_list": "1",
            "product_list": "Electronics;Widget;1;99;",
        }
    ]

    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))

    assert totals == {}
    assert analyzer.stats.purchases_seen == 1
    assert analyzer.stats.purchases_attributed == 0
    assert analyzer.stats.purchases_missing_prior_search == 1
    assert analyzer.stats.revenue_attributed == pytest.approx(0.0)


def test_bad_revenue_value_is_counted_and_ignored(tmp_path: Path) -> None:
    """
    If product_list contains a non numeric revenue value, the analyzer should not crash.
    It should count bad_revenue_values and ignore that revenue.
    """
    input_file = tmp_path / "sample.tsv"

    sample_rows = [
        {
            "ip": "10.0.0.10",
            "user_agent": "UA-BadRevenue",
            "referrer": "https://www.google.com/search?q=ipod",
            "event_list": "",
            "product_list": "",
        },
        {
            "ip": "10.0.0.10",
            "user_agent": "UA-BadRevenue",
            "referrer": "",
            "event_list": "1",
            # revenue field is malformed: "ABC"
            "product_list": "Electronics;Ipod;1;ABC;",
        },
    ]

    write_mini_tsv(input_file, sample_rows)

    analyzer = ExternalSearchRevenueAnalyzer(internal_hosts={"esshopzilla.com"})
    totals = analyzer.run(str(input_file))

    assert totals == {}
    assert analyzer.stats.purchases_seen == 1
    assert analyzer.stats.purchases_attributed == 0
    assert analyzer.stats.bad_revenue_values == 1
    assert analyzer.stats.revenue_attributed == pytest.approx(0.0)
