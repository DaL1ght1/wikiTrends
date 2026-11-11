from docs import doc_id, index_for, to_action
from rows import cells, row_key

WIN = {
    "ts_bucket": "2026-11-11T10:31:00.000Z", "wiki": "enwiki", "title": "Mount Everest",
    "window_size": "1m", "lang": "en", "server_name": "en.wikipedia.org",
    "page_url": "https://en.wikipedia.org/wiki/Mount_Everest", "score": 12.5, "edit_count": 4,
}


def test_doc_id_is_stable_per_window_and_page():
    assert doc_id(WIN) == doc_id(dict(WIN, score=99))  # re-emitted window overwrites
    assert doc_id(WIN) != doc_id(dict(WIN, window_size="5m"))
    assert doc_id(WIN) != doc_id(dict(WIN, ts_bucket="2026-11-11T10:31:30.000Z"))


def test_index_is_daily_by_window_date():
    assert index_for(WIN, "trending-1m", "trending-5m") == "trending-1m-2026.11.11"
    assert index_for(dict(WIN, window_size="5m"), "trending-1m", "trending-5m") == "trending-5m-2026.11.11"
    assert index_for(dict(WIN, ts_bucket=None), "trending-1m", "trending-5m") == "trending-1m"


def test_to_action_sets_url_and_page_key():
    a = to_action(WIN, "trending-1m", "trending-5m")
    assert a["_source"]["url"] == WIN["page_url"]
    assert "page_url" not in a["_source"]
    assert a["_source"]["page_key"] == "enwiki|Mount Everest"
    fallback = to_action(dict(WIN, page_url=None, title="C++"), "trending-1m", "trending-5m")
    assert fallback["_source"]["url"] == "https://en.wikipedia.org/wiki/C%2B%2B"


def test_hbase_row_key_sorts_by_time_within_page():
    k1 = row_key(WIN)
    k2 = row_key(dict(WIN, ts_bucket="2026-11-11T10:32:00.000Z"))
    assert k1 == b"enwiki|Mount Everest|1m|20261111103100"
    assert k1 < k2


def test_hbase_cells_skip_missing_values():
    c = cells(WIN)
    assert c[b"m:score"] == b"12.5"
    assert c[b"m:edit_count"] == b"4"
    assert b"m:z_score" not in c
    assert c[b"i:lang"] == b"en"


def test_bot_and_human_rows_never_collide():
    human = dict(WIN, actor="human")
    bot = dict(WIN, actor="bot")
    assert doc_id(human) == doc_id(WIN)          # v2 ids unchanged for human rows
    assert doc_id(bot) != doc_id(human)
    assert row_key(bot) == row_key(human) + b"|bot"
    assert to_action(WIN, "trending-1m", "trending-5m")["_source"]["actor"] == "human"


def test_bot_account_docs_go_to_their_own_index():
    acc = {"kind": "bot_account", "ts_bucket": "2026-11-11T10:35:00.000Z", "wiki": "dewiki",
           "user": "SchlurcherBot", "window_size": "5m", "edit_count": 120}
    a = to_action(acc, "trending-1m", "trending-5m")
    assert a["_index"] == "trending-bots-5m-2026.11.11"
    assert "page_key" not in a["_source"] and "url" not in a["_source"]
    assert doc_id(acc) == doc_id(dict(acc, edit_count=999))
    assert doc_id(acc) != doc_id(dict(acc, user="OtherBot"))
