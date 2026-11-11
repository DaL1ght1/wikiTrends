from events import Filters, is_interesting, lang_of, page_url, record_key, to_record


def rc(**over):
    base = {
        "type": "edit", "wiki": "enwiki", "title": "Mount Everest", "namespace": 0,
        "server_name": "en.wikipedia.org", "server_url": "https://en.wikipedia.org",
        "meta": {"domain": "en.wikipedia.org", "uri": "https://en.wikipedia.org/wiki/Mount_Everest"},
        "user": "Alice", "bot": False, "minor": False, "timestamp": 1_700_000_000,
        "length": {"old": 100, "new": 150}, "revision": {"old": 1, "new": 2}, "comment": "copyedit",
    }
    base.update(over)
    return base


def test_keeps_edits_and_page_creations():
    f = Filters()
    assert is_interesting(rc(), f)
    assert is_interesting(rc(type="new"), f)
    assert not is_interesting(rc(type="log"), f)
    assert not is_interesting(rc(type="categorize"), f)


def test_drops_canary_and_missing_title():
    f = Filters()
    assert not is_interesting(rc(meta={"domain": "canary"}), f)
    assert not is_interesting(rc(title=""), f)


def test_wiki_include_exclude_and_namespaces():
    f = Filters.from_dict({"exclude_wikis": ["wikidatawiki"], "namespaces": [0]})
    assert not is_interesting(rc(wiki="wikidatawiki"), f)
    assert not is_interesting(rc(namespace=1), f)
    only_fr = Filters.from_dict({"include_wikis": ["fr.*"]})
    assert is_interesting(rc(wiki="frwiki"), only_fr)
    assert not is_interesting(rc(wiki="enwiki"), only_fr)


def test_lang_of_handles_non_language_hosts():
    assert lang_of("en.wikipedia.org") == "en"
    assert lang_of("www.wikidata.org") == "wikidata"
    assert lang_of("commons.wikimedia.org") == "commons"
    assert lang_of(None) == ""


def test_page_url_prefers_meta_uri_and_encodes_fallback():
    assert page_url(rc()) == "https://en.wikipedia.org/wiki/Mount_Everest"
    url = page_url(rc(meta={}, title="C++ (language)"))
    assert url == "https://en.wikipedia.org/wiki/C%2B%2B_(language)"


def test_to_record_and_key():
    r = to_record(rc(comment="Reverted edits by X"))
    assert r["rev_id"] == 2
    assert r["lang"] == "en"
    assert r["is_revert"] is True
    assert r["length_new"] - r["length_old"] == 50
    assert record_key(r) == b"enwiki|Mount Everest"
    assert to_record(rc())["is_revert"] is False
