"""
build_funds_json.py — emit a machine-readable feed of the fund screener.

Reads output/all_funds_metrics.csv (the daily-computed metrics for every fund)
and writes funds.json — one manifest a front-end (e.g. a Stitch-designed MF
screener UI) can fetch to render the sortable table, category filters, the
Expert-Score breakdown and per-fund detail — no XLSX/CSV parsing in the browser.

Writes two copies so it's fetchable wherever GitHub Pages serves from:
    dashboard/funds.json   (next to dashboard/index.html)
    output/funds.json

Run:
    python build_funds_json.py
Add it right after the metrics step in daily_update.py / the Actions workflow.
"""
from __future__ import annotations
import csv, json, datetime as dt
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "output" / "all_funds_metrics.csv"
BASE_URL = "https://myfinancialria.github.io/mf-data-fetcher"

# Expert Score weighting (from README) — surfaced so the UI can explain the score.
SCORE_WEIGHTS = {"returns": 35, "risk": 30, "capture": 15, "alpha": 10, "consistency": 10}

# csv column -> json key. Anything not listed is dropped. Types inferred below.
RENAME = {"scheme_id": "id", "fund_name": "name"}
STR_COLS = {"scheme_id", "fund_name", "category", "nav_date", "inception_date"}
INT_COLS = {"category_rank", "category_total"}
# every other numeric column becomes a float (or null)


def camel(s: str) -> str:
    head, *rest = s.split("_")
    return head + "".join(p.title() for p in rest)


def coerce(col: str, raw: str):
    v = (raw or "").strip()
    if col in STR_COLS:
        return v
    if v == "" or v.lower() in ("nan", "none", "null", "-"):
        return None
    try:
        return int(float(v)) if col in INT_COLS else round(float(v), 3)
    except ValueError:
        return v or None


STALE_DAYS = 10   # a fund whose NAV hasn't updated within this of the latest is dead/merged


def _parse_navdate(s: str):
    try:
        return dt.datetime.strptime((s or "").strip(), "%d-%b-%Y").date()
    except ValueError:
        return None


def load_funds() -> tuple[list[dict], str, int]:
    funds = []
    with SRC.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rec = {}
            for col, raw in row.items():
                if col is None:
                    continue
                key = RENAME.get(col, camel(col))
                rec[key] = coerce(col, raw)
            rec["_navd"] = _parse_navdate(row.get("nav_date", ""))
            funds.append(rec)

    # latest NAV date in the dataset = "as of" reference; flag funds that lag it.
    dates = [f["_navd"] for f in funds if f["_navd"]]
    latest = max(dates) if dates else None
    n_stale = 0
    for f in funds:
        stale = bool(latest and f["_navd"] and (latest - f["_navd"]).days > STALE_DAYS)
        f["stale"] = stale
        n_stale += stale
        del f["_navd"]

    # active funds first (by score), stale/dead funds pushed to the end
    funds.sort(key=lambda f: (f["stale"], f.get("expertScore") is None,
                              -(f.get("expertScore") or 0)))
    nav_date = latest.strftime("%d-%b-%Y") if latest else ""
    return funds, nav_date, n_stale


def category_facets(funds: list[dict]) -> list[dict]:
    counts: dict[str, int] = {}
    for f in funds:
        c = f.get("category") or "Uncategorised"
        counts[c] = counts.get(c, 0) + 1
    return [{"name": c, "count": n}
            for c, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def category_leaders(funds: list[dict], n: int = 5) -> dict:
    """Top-n fund ids per category by expert score (for the 'Top 5' UI strip)."""
    by_cat: dict[str, list[dict]] = {}
    for f in funds:
        if f.get("stale"):
            continue
        by_cat.setdefault(f.get("category") or "Uncategorised", []).append(f)
    leaders = {}
    for cat, items in by_cat.items():
        ranked = sorted(items, key=lambda f: -(f.get("expertScore") or 0))[:n]
        leaders[cat] = [f["id"] for f in ranked]
    return leaders


def main() -> int:
    if not SRC.exists():
        raise SystemExit(f"metrics file not found: {SRC} — run the metrics step first")
    funds, nav_date, n_stale = load_funds()
    active = [f for f in funds if not f["stale"]]
    payload = {
        "generated": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "navDate": nav_date,
        "baseUrl": BASE_URL,
        "count": len(funds),
        "activeCount": len(active),
        "staleCount": n_stale,
        "staleNote": "Funds whose NAV lags the latest session by >10 days are marked "
                     "stale (merged/closed) and excluded from category leaders.",
        "scoreWeights": SCORE_WEIGHTS,
        "categories": category_facets(active),
        "categoryLeaders": category_leaders(funds),
        "funds": funds,
    }
    blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    for dest in (HERE / "dashboard" / "funds.json", HERE / "output" / "funds.json"):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(blob, encoding="utf-8")
        print(f"Wrote {dest.relative_to(HERE)}  ({len(funds)} funds, "
              f"{len(payload['categories'])} categories, nav {nav_date})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
