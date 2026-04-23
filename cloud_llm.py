"""cloud_llm.py — Couche 3 : Gemini 2.0 Flash + Google Search + DuckDB

Pour app_local.py : appelée quand C1 (regex) ne matche rien ET que la
question ne concerne pas un ticker individuel ou un engulfing (ceux-ci
sont gérés ailleurs — cf. règle split avec conv "Engulfing / Beta>2").

Usage:
    from cloud_llm import answer_question
    result = answer_question("quel impact a eu l'élection PM japonais sur le Nikkei ?")
    # → {"type": "LLM_CLOUD", "ok": True, "answer": "...", "df": None, "sources": [...]}

Dépendances : google-genai, python-dotenv, duckdb, pandas.
Clef API : GEMINI_API_KEY dans .env (gitignored).
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "live_selected"

load_dotenv(BASE_DIR / ".env")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

MODEL = "gemini-2.5-flash"  # 2026 : modèle courant avec grounding Google Search
MAX_TOOL_ITERATIONS = 6  # Garde-fou contre boucles infinies


# ─── Guards : questions à NE PAS traiter ─────────────────────────────────

# Mots-clés BBE : gérés par l'onglet BBE dédié, pas par C3.
_BBE_KEYWORDS = re.compile(
    r"\b(engulfing|bearish\s*e|bullish\s*e|bbe|avaleur|avalement)\b",
    re.IGNORECASE,
)

# Détection d'un ticker individuel dans la question. On pioche la liste
# des tickers depuis le dossier tickers/ (géré par l'autre conv) + une
# shortlist de synonymes courants.
_TICKER_SYNONYMS = {
    "apple": "AAPL", "google": "GOOG", "alphabet": "GOOG",
    "nvidia": "NVDA", "reddit": "RDDT", "robinhood": "HOOD",
    "applovin": "APP", "ondas": "ONDS", "iren": "IREN",
    "meli": "MELI", "mercadolibre": "MELI", "mu": "MU",
    "micron": "MU", "coherent": "COHR", "aaoi": "AAOI",
}


def _known_tickers() -> set[str]:
    tickers_dir = DATA_DIR / "tickers"
    if not tickers_dir.exists():
        return set()
    found = set()
    for p in tickers_dir.glob("*.csv"):
        stem = p.stem
        if stem.endswith("_earnings"):
            stem = stem[: -len("_earnings")]
        found.add(stem.upper())
    return found


def _detect_ticker_or_bbe(question: str) -> str | None:
    """Retourne un message d'info si la question doit aller ailleurs, sinon None."""
    q = question.strip()
    q_lower = q.lower()

    # BBE check
    if _BBE_KEYWORDS.search(q_lower):
        return (
            "Cette question concerne un pattern Bearish/Bullish Engulfing. "
            "Utilise l'onglet **🕯️ BBE — Analyse multi-ticker** en haut de "
            "la page pour sélectionner le ticker, le sens (bearish/bullish), "
            "la fenêtre J+1..J+5 et le seuil."
        )

    # Synonymes ticker
    for syn, tk in _TICKER_SYNONYMS.items():
        if re.search(rf"\b{re.escape(syn)}\b", q_lower):
            return (
                f"Cette question concerne un ticker individuel ({tk}). "
                "Tape simplement le symbole ou utilise l'onglet BBE. "
                "Ce champ de recherche est dédié aux indices, VIX, macro et "
                "questions générales."
            )

    # Tickers explicites (AAPL, GOOG, etc.)
    tickers = _known_tickers()
    if tickers:
        # cherche un mot de 2-5 majuscules qui matche un ticker connu
        for m in re.finditer(r"\b([A-Z]{2,5})\b", q):
            if m.group(1) in tickers:
                return (
                    f"Cette question concerne un ticker individuel ({m.group(1)}). "
                    "Tape simplement le symbole ou utilise l'onglet BBE. "
                    "Ce champ de recherche est dédié aux indices, VIX, macro et "
                    "questions générales."
                )

    return None


# ─── Schéma DuckDB : introspection une fois au boot ──────────────────────

_SCHEMA_CACHE: str | None = None


def _build_duckdb_schema() -> tuple[str, duckdb.DuckDBPyConnection]:
    """Charge tous les *_daily.csv en tables DuckDB + retourne résumé texte."""
    conn = duckdb.connect(":memory:")
    lines = []

    daily_files = sorted(p for p in DATA_DIR.glob("*_daily.csv"))
    for p in daily_files:
        table_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.stem.lower())
        try:
            # Auto-detect séparateur
            df = pd.read_csv(p, sep=None, engine="python", nrows=5)
            sep = ";" if ";" in open(p, encoding="utf-8", errors="ignore").readline() else ","
            conn.execute(
                f"CREATE TABLE {table_name} AS "
                f"SELECT * FROM read_csv_auto('{p}', sep='{sep}', header=true)"
            )
            # Récup colonnes + plage dates
            cols = conn.execute(f"DESCRIBE {table_name}").fetchdf()
            col_list = ", ".join(cols["column_name"].tolist())
            # Plage temporelle si colonne time
            date_range = ""
            if "time" in cols["column_name"].values:
                try:
                    rg = conn.execute(
                        f"SELECT MIN(time) AS mn, MAX(time) AS mx, COUNT(*) AS n FROM {table_name}"
                    ).fetchone()
                    date_range = f" — {rg[0]} → {rg[1]} ({rg[2]} lignes)"
                except Exception:
                    pass
            lines.append(f"- **{table_name}** ({col_list}){date_range}")
        except Exception as e:
            lines.append(f"- ~~{table_name}~~ (chargement échoué: {e})")

    schema_text = "\n".join(lines)
    return schema_text, conn


def _get_schema_and_conn() -> tuple[str, duckdb.DuckDBPyConnection]:
    """Rebuild à chaque appel — en usage Streamlit on peut cacher plus tard."""
    return _build_duckdb_schema()


# ─── Appel Gemini avec outils ────────────────────────────────────────────

_WEB_KEYWORDS = re.compile(
    r"\b(election|élu|élue|premier\s*ministre|président|gouverneur|"
    r"actualité|news|récent|récente|hier|aujourd'?hui|cette\s*semaine|"
    r"cette\s*année|depuis\s*(janvier|février|mars|avril|mai|juin|juillet|"
    r"août|septembre|octobre|novembre|décembre)|crise|guerre|annonce|"
    r"tarif|tariff|trump|biden|fed\s*meeting|fomc\s*meeting|publication|"
    r"discours|powell|lagarde|nomination|démission|accord|traité|sanction|"
    r"conflit|attentat|coup\s*d'état|scandale|ipo|fusion|acquisition|"
    r"rachat|faillite|scandale|who\s+(is|was)|qui\s+est|qui\s+a\s+été|"
    r"when\s+(was|did)|quand\s+a-t-il|quand\s+a-t-elle|que\s+s'est-il\s+passé|"
    # Questions de définition / connaissance générale → web grounding aussi
    r"qu'?est[- ]?ce\s+qu|c'?est\s+quoi|que\s+signifie|signifie\s+quoi|"
    r"définition|définir|explique|explication|explique[- ]?moi|"
    r"what\s+is|what\s+does|define)\b",
    re.IGNORECASE,
)


def _needs_web(question: str) -> bool:
    return bool(_WEB_KEYWORDS.search(question))


def _call_gemini_with_tools(question: str, schema: str, conn) -> dict[str, Any]:
    """Dialogue multi-tour. 2.5+ interdit grounding+tools dans le même appel,
    donc on choisit UN mode selon la question :
      - web_mode=True : Google Search grounding seul (pas de SQL)
      - web_mode=False : execute_sql seul (pas de grounding)
    """
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=GEMINI_API_KEY)
    web_mode = _needs_web(question)

    if web_mode:
        tools = [types.Tool(google_search=types.GoogleSearch())]
        tool_hint = ("You have Google Search grounding. Use it to find current "
                     "facts. Then answer concisely in French with sources.")
    else:
        execute_sql_decl = types.FunctionDeclaration(
            name="execute_sql",
            description=(
                "Execute a DuckDB SQL query against the loaded market data "
                "tables. Returns JSON records. Use for historical market data."
            ),
            parameters=types.Schema(
                type="OBJECT",
                properties={
                    "sql": types.Schema(
                        type="STRING",
                        description="DuckDB SQL query.",
                    )
                },
                required=["sql"],
            ),
        )
        tools = [types.Tool(function_declarations=[execute_sql_decl])]
        tool_hint = (f"You have execute_sql against a DuckDB database with these "
                     f"tables:\n\n{schema}\n\n"
                     "Rules for SQL:\n"
                     "- Column names are case-sensitive as shown.\n"
                     "- For dates, use CAST(time AS DATE).\n"
                     "- Never query tables that don't exist.\n"
                     "- For a day's variation use (close-open)/open*100.\n"
                     "- For close-to-close variation use "
                     "(close - LAG(close) OVER (ORDER BY time))/LAG(close) OVER (ORDER BY time)*100.\n"
                     "- If the question doesn't require querying the database "
                     "(e.g. a definition, a general finance concept, a formula "
                     "explanation), answer directly from your own knowledge "
                     "WITHOUT calling execute_sql. Do NOT refuse to answer by "
                     "claiming you only do SQL.")

    system_instruction = f"""You are the research assistant for the SPX Quant Engine app.
The user writes in French. Answer concisely in French, using markdown.

{tool_hint}

Additional rules:
- Keep answer under 300 words unless more detail is explicitly requested.
- Never invent data. If the tool fails or data is missing, say so clearly.
- NEVER answer questions about individual US tickers (AAPL, GOOG, NVDA, etc.) or "bearish/bullish engulfing" patterns — those are handled by other parts of the app. Redirect politely.
"""

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        tools=tools,
        temperature=0.2,
    )

    contents = [types.Content(role="user", parts=[types.Part(text=question)])]
    sql_executed: list[dict] = []
    last_df: pd.DataFrame | None = None

    for _ in range(MAX_TOOL_ITERATIONS):
        resp = client.models.generate_content(
            model=MODEL, contents=contents, config=config,
        )

        # Un seul candidat en pratique
        cand = resp.candidates[0] if resp.candidates else None
        if not cand or not cand.content or not cand.content.parts:
            return {"ok": False, "answer": "(Réponse vide du modèle)", "sql": sql_executed, "df": last_df}

        # Cherche un function_call dans la réponse
        fn_call = None
        text_parts = []
        for part in cand.content.parts:
            if getattr(part, "function_call", None):
                fn_call = part.function_call
            elif getattr(part, "text", None):
                text_parts.append(part.text)

        if fn_call is None:
            # Réponse finale (texte)
            answer = "\n".join(text_parts).strip() or "(réponse vide)"
            # Sources (grounding metadata)
            sources = []
            if hasattr(cand, "grounding_metadata") and cand.grounding_metadata:
                gm = cand.grounding_metadata
                if hasattr(gm, "grounding_chunks") and gm.grounding_chunks:
                    for ch in gm.grounding_chunks:
                        if hasattr(ch, "web") and ch.web:
                            sources.append({"title": ch.web.title, "uri": ch.web.uri})
            return {
                "ok": True,
                "answer": answer,
                "sql": sql_executed,
                "df": last_df,
                "sources": sources,
            }

        # Sinon : exécute l'appel SQL et rejoue
        if fn_call.name == "execute_sql":
            sql = fn_call.args.get("sql", "") if fn_call.args else ""
            try:
                result_df = conn.execute(sql).fetchdf()
                result_records = result_df.head(200).to_dict(orient="records")
                last_df = result_df
                sql_executed.append({"sql": sql, "rows": len(result_df)})
                tool_response = {"rows": result_records, "n_rows": len(result_df)}
            except Exception as e:
                sql_executed.append({"sql": sql, "error": str(e)})
                tool_response = {"error": str(e)}

            contents.append(cand.content)
            contents.append(
                types.Content(
                    role="user",
                    parts=[
                        types.Part.from_function_response(
                            name="execute_sql",
                            response=tool_response,
                        )
                    ],
                )
            )
            continue

        # Outil inconnu
        return {
            "ok": False,
            "answer": f"(Le modèle a appelé un outil inconnu : {fn_call.name})",
            "sql": sql_executed,
            "df": last_df,
        }

    return {
        "ok": False,
        "answer": f"(Trop d'itérations tool-call — plafond {MAX_TOOL_ITERATIONS})",
        "sql": sql_executed,
        "df": last_df,
    }


# ─── Point d'entrée public ───────────────────────────────────────────────

def answer_question(question: str) -> dict[str, Any]:
    """Route principale. Retourne un dict compatible avec le rendu app_local.py.

    Format de retour :
      {
        "type": "LLM_CLOUD" | "REDIRECT",
        "ok": bool,
        "answer": str (markdown),
        "df": pd.DataFrame | None,
        "sql": list[dict],   # [{"sql": "...", "rows": N} | {"sql": "...", "error": "..."}]
        "sources": list[dict],  # [{"title": "...", "uri": "..."}]
      }
    """
    if not GEMINI_API_KEY:
        return {
            "type": "LLM_CLOUD", "ok": False,
            "answer": "⚠️ GEMINI_API_KEY non configurée dans .env",
            "df": None, "sql": [], "sources": [],
        }

    # Guard : BBE / ticker individuel
    redirect = _detect_ticker_or_bbe(question)
    if redirect:
        return {
            "type": "REDIRECT", "ok": True,
            "answer": redirect,
            "df": None, "sql": [], "sources": [],
        }

    # Schéma + conn DuckDB
    try:
        schema, conn = _get_schema_and_conn()
    except Exception as e:
        return {
            "type": "LLM_CLOUD", "ok": False,
            "answer": f"Erreur chargement données : {e}",
            "df": None, "sql": [], "sources": [],
        }

    try:
        result = _call_gemini_with_tools(question, schema, conn)
    except Exception as e:
        return {
            "type": "LLM_CLOUD", "ok": False,
            "answer": f"Erreur Gemini : {type(e).__name__}: {e}",
            "df": None, "sql": [], "sources": [],
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass

    result["type"] = "LLM_CLOUD"
    return result


# ─── Test CLI rapide ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else "nombre de jours SPX supérieur à 3% en 2022"
    print(f"Q: {q}\n")
    r = answer_question(q)
    print(f"Type: {r['type']}  |  OK: {r['ok']}")
    print(f"Answer:\n{r['answer']}\n")
    if r.get("sql"):
        print(f"SQL exécuté ({len(r['sql'])} fois):")
        for s in r["sql"]:
            print(f"  {s}")
    if r.get("sources"):
        print(f"Sources web ({len(r['sources'])}):")
        for s in r["sources"][:5]:
            print(f"  - {s.get('title')}: {s.get('uri')}")
