"""
Régression — SPX Quant Engine v2.4.0
Questions-clés validées. Lance avec : python3 -m pytest tests/test_regression.py -v
"""
import sys, math
sys.path.insert(0, "/Users/yann/spx-quant-engine")

import pytest
import app_local

# ─── Fixture : DuckDB chargé une seule fois ───────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def duckdb_loaded():
    app_local._ensure_duckdb()


# ─── Helpers ──────────────────────────────────────────────────────────────

def lookup(q):
    return app_local._compute_lookup(q)

def compute(q):
    return app_local._compute_result(q)

def approx(val, expected, rel=0.01):
    """Tolérance relative 1 % par défaut."""
    return math.isclose(val, expected, rel_tol=rel)


# ─── Tests ────────────────────────────────────────────────────────────────

def test_perf_spx_2022():
    """Performance SPX 2022 ≈ -19.64 %"""
    r = lookup("performance SPX 2022")
    assert r is not None and r["type"] == "C1_LOOKUP" and r["ok"]
    assert approx(r["value"], -19.64, rel=0.02), f"got {r['value']}"


def test_perf_spx_2023():
    """Performance SPX 2023 ≈ +23.79 %"""
    r = lookup("performance SPX 2023")
    assert r is not None and r["ok"]
    assert approx(r["value"], 23.79, rel=0.02), f"got {r['value']}"


def test_perf_spx_2024():
    """Performance SPX 2024 ≈ +23.95 %"""
    r = lookup("performance SPX 2024")
    assert r is not None and r["ok"]
    assert approx(r["value"], 23.95, rel=0.02), f"got {r['value']}"


def test_cloture_spx_oct_2025():
    """Clôture SPX le 9 octobre 2025 = 6735.11"""
    r = lookup("clôture SPX le 9 octobre 2025")
    assert r is not None and r["type"] == "C1_LOOKUP" and r["ok"]
    assert approx(r["value"], 6735.11, rel=0.001), f"got {r['value']}"


def test_open_vix_mars_2024():
    """Open VIX le 15 mars 2024 = 14.33"""
    r = lookup("open VIX le 15 mars 2024")
    assert r is not None and r["ok"]
    assert approx(r["value"], 14.33, rel=0.01), f"got {r['value']}"


def test_compare_spx_2023_vs_2024():
    """Comparaison SPX 2023 vs 2024 : left≈23.79%, right≈23.95%"""
    r = compute("performance SPX 2023 vs performance SPX 2024")
    assert r["type"] == "C1_COMPARE" and r["ok"]
    lv = r["left"]["result"]["value"]
    rv = r["right"]["result"]["value"]
    assert approx(lv, 23.79, rel=0.02), f"left got {lv}"
    assert approx(rv, 23.95, rel=0.02), f"right got {rv}"


def test_c1_spx_vix_gt18():
    """SPX quand VIX > 18 : environ 648 jours"""
    r = app_local.layer1_structured("SPX quand VIX > 18")
    assert r is not None and r["type"] == "C1"
    n = r["n"]
    assert 600 <= n <= 700, f"got n={n}"


def test_c1_spx_weekday():
    """SPX quand VIX > 18 les lundis : résultat C1 non vide"""
    r = app_local.layer1_structured("SPX quand VIX > 18 les lundis")
    assert r is not None and r["type"] == "C1"
    assert r["n"] > 0


def test_ic_ric_detection():
    """IC aile 10 VIX 17.16 → type IC_RIC ok"""
    r = app_local._compute_ic_ric("IC aile 10 VIX 17.16")
    assert r is not None and r["type"] == "IC_RIC" and r["ok"]
    # crédit attendu ≈ 8.40 (tolérance large car dépend du CSV)
    assert isinstance(r["credit"], float)
    assert r["wing"] == 10


def test_lookup_perf_month():
    """Performance SPX mars 2024 : résultat C1_LOOKUP ok"""
    r = lookup("performance SPX mars 2024")
    assert r is not None and r["type"] == "C1_LOOKUP" and r["ok"]
    assert r["unit"] == "%"
    assert isinstance(r["value"], float)


# ─── Tests v2.10 ────────────────────────────────────────────────────────

def test_multi_condition_classify():
    from query_interpreter import interpret_query
    r = interpret_query("quand le VIX est supérieur à 25 et que AAOI a baissé de 5%")
    assert r["category"] == "MULTI_CONDITION"

def test_engulfing_by_year_classify():
    from query_interpreter import interpret_query
    r = interpret_query("combien de fois le BE a fonctionné chaque année de 2022 à 2025 sur AAOI")
    assert r["category"] == "ENGULFING_ANALYSIS"

def test_relative_period_months():
    from query_interpreter import _detect_period
    p = _detect_period("depuis 6 mois")
    assert p is not None and "date_from" in p

def test_relative_period_ytd():
    from query_interpreter import _detect_period
    p = _detect_period("depuis le début de l'année")
    assert p is not None and "date_from" in p

def test_be_alias():
    from query_interpreter import interpret_query
    r = interpret_query("combien de fois le BE a marché sur AAOI")
    assert r["category"] in ("ENGULFING_ANALYSIS", "CANDLE_PATTERN")

def test_lookup_best_3years():
    from query_interpreter import interpret_query
    r = interpret_query("meilleur jour AAOI en 2023 et 2024 et 2025")
    assert r["category"] == "LOOKUP_BEST"
    assert len(r.get("period", {}).get("years", [])) == 3

def test_filter_abs():
    from query_interpreter import interpret_query
    r = interpret_query("AAOI a bougé de plus de 10%")
    assert r["category"] == "FILTER_STATS"
    assert r.get("criterion") == "abs"

def test_spx_overnight_import():
    from spx_patterns import get_all_patterns
    patterns = get_all_patterns()
    assert isinstance(patterns, list)

def test_followup_context():
    from query_interpreter import interpret_query
    r = interpret_query("trouve les points communs aux échecs",
                        active_ticker="AAOI",
                        last_category="ENGULFING_ANALYSIS",
                        last_params={"pattern": "bearish_engulfing", "seuil": 2.0})
    assert r["category"] == "ENGULFING_FAILURE_ANALYSIS"

def test_enriched_lookup():
    r = compute("clôture AAOI le 4 août 2023")
    assert r["type"] == "INTERPRETED" and r["ok"]
    assert r.get("sub_type") == "single_value_enriched"
    assert "context" in r
    assert r["context"].get("var_pct") is not None
