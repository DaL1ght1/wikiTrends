import pytest

from queries import shape_summary, shape_trending, summary, trending, window_cfg


def test_window_cfg_rejects_unknown():
    assert window_cfg("1m") == ("trending-1m", "30s", 2)
    with pytest.raises(ValueError):
        window_cfg("2m")


def test_trending_query_orders_by_peak_score_and_applies_filters():
    q = trending("1m", 10, 25, "enwiki", "election")
    terms = q["aggs"]["pages"]["terms"]
    assert terms["field"] == "page_key"
    assert terms["order"] == {"peak_score": "desc"}
    filters = q["query"]["bool"]["filter"]
    assert {"term": {"wiki": "enwiki"}} in filters
    assert any("match" in f for f in filters)
    assert q["aggs"]["pages"]["aggs"]["spark"]["date_histogram"]["fixed_interval"] == "30s"


def test_trending_query_without_filters_only_has_time_range():
    q = trending("5m", 30, 10, None, None)
    assert q["query"]["bool"]["filter"] == [{"range": {"last_event_ts": {"gte": "now-30m"}}}]


def test_summary_timeline_uses_one_bucket_per_slide():
    q = summary("5m", 60)
    assert q["aggs"]["timeline"]["date_histogram"]["fixed_interval"] == "60s"


def _bucket(key, title, score, spark):
    return {
        "key": key, "peak_score": {"value": score}, "peak_z": {"value": 4.2},
        "peak_edits": {"value": 7}, "peak_editors": {"value": 3}, "new_page": {"value": 1},
        "latest": {"hits": {"hits": [{"_source": {"wiki": "enwiki", "title": title}}]}},
        "spark": {"buckets": [{"edits": {"value": v}} for v in spark]},
    }


def test_shape_trending():
    resp = {"aggregations": {"pages": {"buckets": [_bucket("enwiki|A", "A", 10.0, [1, None, 3])]}}}
    [p] = shape_trending(resp)
    assert p["title"] == "A"
    assert p["page_key"] == "enwiki|A"
    assert p["is_new"] is True
    assert p["spark"] == [1, 0, 3]


def test_shape_summary_divides_overlap_and_skips_partial_bucket():
    resp = {"aggregations": {
        "timeline": {"buckets": [
            {"key": 1, "edits": {"value": 100}, "pages": {"value": 50}},
            {"key": 2, "edits": {"value": 120}, "pages": {"value": 60}},
            {"key": 3, "edits": {"value": 30}, "pages": {"value": 20}},  # still filling
        ]},
        "recent": {"pages": {"value": 80}, "wikis": {"value": 12},
                   "latency": {"values": {"50.0": 1500.0, "95.0": 4000.0}}},
        "freshest": {"value_as_string": "2026-11-11T10:31:00.000Z"},
        "top_wikis": {"buckets": [{"key": "enwiki", "edits": {"value": 1200}}]},
        "spikes": {"pages": {"value": 2}},
    }}
    s = shape_summary(resp, "1m", 10)
    assert s["edits_per_window"] == 120
    # only 3 buckets x 30 s = 1.5 min of the 10 min period have data:
    # 1200 / overlap 2 / 1.5 min = 400 edits per minute
    assert s["covered_minutes"] == 1.5
    assert s["top_wikis"] == [{"wiki": "enwiki", "edits_per_min": 400.0}]
    assert s["latency_p50_ms"] == 1500.0
    assert s["spiking_pages"] == 2


def test_shape_summary_empty_response():
    s = shape_summary({}, "1m", 10)
    assert s["timeline"] == [] and s["edits_per_window"] == 0 and s["top_wikis"] == []


def test_human_views_exclude_bots_but_keep_legacy_docs():
    q = trending("1m", 10, 25, None, None)
    # must_not (not a term on "human"), so docs without an actor field still match
    assert q["query"]["bool"]["must_not"] == [{"term": {"actor": "bot"}}]
    assert summary("1m", 10)["query"]["bool"]["must_not"] == [{"term": {"actor": "bot"}}]


def test_shape_bots():
    from queries import shape_bots

    def tb(key, human, bot):
        return {"key": key, "human": {"edits": {"value": human}}, "bot": {"edits": {"value": bot}}}

    win = {"aggregations": {
        "timeline": {"buckets": [tb(1, 100, 60), tb(2, 120, 80), tb(3, 10, 5)]},
        "wikis": {"buckets": [
            {"key": "cebwiki", "bot": {"edits": {"value": 90}}, "all": {"value": 100}},
            {"key": "enwiki", "bot": {"edits": {"value": 0}}, "all": {"value": 500}},
        ]},
        "pages": {"top": {"buckets": [{"key": "dewiki|X", "edits": {"value": 10},
                                       "latest": {"hits": {"hits": [{"_source": {"wiki": "dewiki", "title": "X"}}]}}}]}},
    }}
    acc = {"aggregations": {
        "active": {"value": 7},
        "accounts": {"buckets": [{
            "key": "SchlurcherBot", "edits": {"value": 750}, "wikis": {"value": 3},
            "peak_pages": {"value": 140}, "new_pages": {"value": 0},
            "top_wiki": {"buckets": [{"key": "commonswiki", "srv": {"hits": {"hits": [
                {"_source": {"server_name": "commons.wikimedia.org"}}]}}}]},
        }]},
    }}
    b = shape_bots(win, acc, 30)
    assert b["covered_minutes"] == 1.5                     # 3 buckets x 30 s
    assert b["bot_edits_per_min"] == 80                    # newest complete bucket
    assert b["bot_share"] == round(145 / 375, 3)
    assert b["active_bots"] == 7
    assert [w["wiki"] for w in b["wikis"]] == ["cebwiki"]  # wikis without bot edits are dropped
    assert b["wikis"][0]["bot_share"] == 0.9
    assert b["accounts"][0]["edits_per_min"] == 100.0      # 750 / overlap 5 / 1.5 min
    assert b["accounts"][0]["server_name"] == "commons.wikimedia.org"
    assert b["pages"][0]["bot_edits"] == 5                 # 10 / overlap 2


def test_spark_interval_keeps_bucket_count_bounded():
    from queries import spark_interval
    assert spark_interval("1m", 10) == "30s"     # short periods: one point per slide
    assert spark_interval("1m", 180) == "360s"   # 3 h -> 30 points
    assert spark_interval("5m", 10) == "60s"     # never finer than the 5m slide
    assert spark_interval("1m", 17) == "60s"     # rounded up to a multiple of the slide
