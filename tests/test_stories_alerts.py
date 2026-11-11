from alerts import AlertSettings, alert_id, spike_alerts, spike_query, story_alerts, webhook_payload
from stories import build_stories


def page(wiki, title, score, server=None, lang=None, spark=(1, 2), z=2.0, edits=3, editors=2):
    lang = lang or wiki.replace("wiki", "")
    return {"wiki": wiki, "title": title, "lang": lang, "server_name": server or f"{lang}.wikipedia.org",
            "url": f"https://{lang}.wikipedia.org/wiki/{title}", "peak_score": score, "peak_z": z,
            "peak_edits": edits, "peak_editors": editors, "spark": list(spark), "is_new": False,
            "last_event_ts": "2026-11-11T10:00:00.000Z"}


def info_for(pages, qids, en=None):
    en = en or {}
    return {(p["server_name"], p["title"]): {"qid": qids.get(p["title"]), "title_en": en.get(p["title"])}
            for p in pages}


def test_stories_group_by_wikidata_item_across_languages():
    pages = [
        page("dewiki", "Erdbeben in X", 9), page("frwiki", "Séisme de X", 7),
        page("enwiki", "X earthquake", 5), page("eswiki", "Terremoto de X", 4, spark=(0, 1, 5)),
        page("enwiki", "Lonely page", 20),                       # one language only: not a story
        page("dewiki", "Erdbeben X (Weiterleitung)", 1),         # same item, same wiki: deduped
    ]
    qids = {"Erdbeben in X": "Q1", "Séisme de X": "Q1", "X earthquake": "Q1", "Terremoto de X": "Q1",
            "Erdbeben X (Weiterleitung)": "Q1", "Lonely page": "Q2"}
    [story] = build_stories(pages, info_for(pages, qids))
    assert story["qid"] == "Q1"
    assert story["languages"] == 4
    assert story["headline"] == "X earthquake" and story["headline_lang"] == "en"   # enwiki title preferred
    assert [e["wiki"] for e in story["editions"]] == ["dewiki", "frwiki", "enwiki", "eswiki"]
    assert story["score"] == 25
    assert story["spark"] == [3, 7, 5]                           # element-wise sum, ragged lengths ok


def test_story_headline_falls_back_to_english_title_then_top_page():
    pages = [page("dewiki", "A", 5), page("frwiki", "B", 3)]
    s = build_stories(pages, info_for(pages, {"A": "Q9", "B": "Q9"}, en={"B": "Bee"}))[0]
    assert (s["headline"], s["headline_lang"]) == ("Bee", "en")
    s = build_stories(pages, info_for(pages, {"A": "Q9", "B": "Q9"}))[0]
    assert (s["headline"], s["headline_lang"]) == ("A", "de")


def test_three_language_stories_rank_first():
    pages = [page("dewiki", "a", 50), page("frwiki", "b", 50),
             page("dewiki", "c", 1), page("frwiki", "d", 1), page("eswiki", "e", 1)]
    stories = build_stories(pages, info_for(pages, {"a": "Q1", "b": "Q1", "c": "Q2", "d": "Q2", "e": "Q2"}))
    assert [s["qid"] for s in stories] == ["Q2", "Q1"]


def test_alert_ids_are_stable_within_cooldown():
    assert alert_id("spike", "k", 3600 * 5 + 10, 60) == alert_id("spike", "k", 3600 * 5 + 3500, 60)
    assert alert_id("spike", "k", 3600 * 5, 60) != alert_id("spike", "k", 3600 * 6, 60)
    assert alert_id("spike", "k", 0, 60) != alert_id("story", "k", 0, 60)


def test_spike_query_excludes_bots_and_applies_thresholds():
    q = spike_query(5, 2)
    filters = q["query"]["bool"]["filter"]
    assert {"range": {"z_score": {"gte": 5}}} in filters
    assert {"range": {"unique_editors": {"gte": 2}}} in filters
    assert q["query"]["bool"]["must_not"] == [{"term": {"actor": "bot"}}]


def test_spike_and_story_alerts():
    s = AlertSettings(min_languages=3)
    resp = {"aggregations": {"pages": {"buckets": [{"key": "enwiki|X", "latest": {"hits": {"hits": [{"_source": {
        "wiki": "enwiki", "title": "X", "z_score": 7.4, "edit_count": 9, "unique_editors": 4}}]}}}]}}}
    [a] = spike_alerts(resp, s, now=0)
    assert a["kind"] == "spike" and a["headline"] == "X" and "7.4 standard deviations" in a["reason"]

    pages = [page("dewiki", "a", 5), page("frwiki", "b", 5), page("eswiki", "c", 5)]
    stories = build_stories(pages, info_for(pages, {"a": "Q1", "b": "Q1", "c": "Q1"}))
    [b] = story_alerts(stories, s, now=0)
    assert b["kind"] == "story" and b["languages"] == 3 and "3 languages" in b["reason"]
    assert story_alerts(stories, AlertSettings(min_languages=4), now=0) == []


def test_webhook_payload_works_for_slack_and_discord():
    p = webhook_payload({"kind": "story", "headline": "X", "reason": "r", "url": "https://u"}, "http://ui")
    assert p["text"].startswith("🌍 *X*") and p["content"].startswith("🌍 **X**")
    assert "https://u" in p["text"] and "http://ui" in p["text"]
