from titles import PageInfo, chunks, eligible, english_server, parse_page_info, queryable


def test_queryable_and_eligible():
    assert queryable("sr.wikipedia.org") and eligible("sr.wikipedia.org")
    assert queryable("de.wikivoyage.org") and eligible("de.wikivoyage.org")
    # English editions: no English title needed, but their Wikidata id still matters
    assert queryable("en.wikipedia.org") and not eligible("en.wikipedia.org")
    assert queryable("simple.wikipedia.org") and not eligible("simple.wikipedia.org")
    for host in ("fr.wiktionary.org", "www.wikidata.org", "commons.wikimedia.org",
                 "incubator.wikimedia.org", None):
        assert not queryable(host) and not eligible(host)


def test_english_server():
    assert english_server("sr.wikipedia.org") == "en.wikipedia.org"
    assert english_server("de.wikivoyage.org") == "en.wikivoyage.org"


def test_chunks():
    assert [len(c) for c in chunks([str(i) for i in range(120)])] == [50, 50, 20]


def test_parse_page_info_traces_normalization_and_redirects():
    resp = {"query": {
        "normalized": [{"from": "апшеронско_полуострво", "to": "Апшеронско полуострво"}],
        "redirects": [{"from": "Old name", "to": "New name"}],
        "pages": [
            {"title": "Апшеронско полуострво", "pageprops": {"wikibase_item": "Q192570"},
             "langlinks": [{"lang": "en", "title": "Absheron Peninsula"}]},
            {"title": "New name", "langlinks": [{"lang": "en", "title": "English name"}]},
            {"title": "Brand new page"},
            {"title": "Missing", "missing": True},
        ],
    }}
    out = parse_page_info(resp, ["апшеронско_полуострво", "Old name", "Brand new page", "Missing", "Never returned"])
    assert out["апшеронско_полуострво"] == {"title_en": "Absheron Peninsula", "qid": "Q192570"}
    assert out["Old name"] == {"title_en": "English name", "qid": None}
    assert out["Brand new page"] == {"title_en": None, "qid": None}
    assert out["Never returned"] == {"title_en": None, "qid": None}


def test_lookup_uses_cache_and_skips_unqueryable(monkeypatch):
    calls = []
    pi = PageInfo("test-agent")

    def fake_fetch(server, titles):
        calls.append((server, tuple(titles)))
        for t in titles:
            pi._put((server, t), {"title_en": f"EN {t}" if t != "x" else None, "qid": f"Q{len(t)}"})
        with pi._lock:
            pi._inflight.difference_update((server, t) for t in titles)

    monkeypatch.setattr(pi, "_fetch", fake_fetch)
    pages = [("sr.wikipedia.org", "a"), ("sr.wikipedia.org", "x"), ("en.wikipedia.org", "b"), ("fr.wiktionary.org", "c")]
    first = pi.lookup(pages)
    assert set(first) == {("sr.wikipedia.org", "a"), ("sr.wikipedia.org", "x"), ("en.wikipedia.org", "b")}
    assert first[("sr.wikipedia.org", "a")]["title_en"] == "EN a"
    assert sorted(calls) == [("en.wikipedia.org", ("b",)), ("sr.wikipedia.org", ("a", "x"))]
    assert pi.lookup(pages) == first
    assert len(calls) == 2  # second call served from cache
