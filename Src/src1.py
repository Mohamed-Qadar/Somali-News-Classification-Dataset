#!/usr/bin/env python3
"""
Somali News Classification Dataset Builder (Balanced)

Goal

Build (or extend) a Somali news headline dataset for text classification with balanced labels,
using multiple Somali-language news sources.

This script was written to be:
- Readable and easy to modify for new researchers
- Config-driven (add/remove sources without touching core logic)
- Safe to interrupt (periodic checkpoints)
- Duplicate-aware (normalizes titles and de-duplicates)

What it collects

- Headline text (default: title)
- label (Politics / World / Sports / Economy)
- source (site name)
- url (article URL, when available)

Typical workflow

1) Start from an existing CSV (e.g., Politics & World already balanced).
2) Add Sports and Economy until each label reaches TARGET_PER_LABEL.
3) Save a final balanced dataset.

IMPORTANT

- Site structures can change. If a source stops working, update its selectors or base URL.
- Respect robots.txt / fair use policies and keep request rates conservative.

Author

You (Mr Abdi) + community contributors.
"""

from __future__ import annotations

import os
import sys
import time
import random
import argparse
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup



# Configuration (EDIT THIS ONLY)


TARGET_PER_LABEL = 5255

TEXT_COL = "text"
LABEL_COL = "label"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TIMEOUT_SECONDS = 25

# Be polite: random sleep between requests
SLEEP_MIN = 0.5
SLEEP_MAX = 1.2

# Save progress every N added rows
CHECKPOINT_EVERY = 300

# Stop a source if we get too many pages with 0 additions (likely duplicates / category ended)
STOP_AFTER_ZERO_STREAK = 80

# Economy keyword filter for sources that do not have a clean Economy category
ECONOMY_KEYWORDS = [
    # Somali
    "dhaqaale", "dhaqaalaha", "dhaqaaleed", "ganacsi", "ganacsiga", "ganacsato",
    "sicir", "sicir-barar", "sicirbarar", "qiime", "qiimaha", "kharash", "miisaaniyad",
    "dakhli", "khasaaro", "faa'iido", "faaido", "deyn", "dayn", "maalgashi", "maalgelin",
    "shirkad", "shirkadaha", "saami", "saamiga", "saamiyo",
    "suq", "suuq", "suqyada", "suuqyada",
    "deked", "dhoof", "dhoofin", "soo dejin", "soodejin", "soodajin",
    "xawaalad", "lacag", "shilin", "doolar", "sarif", "sarrif",
    "bangiga", "bangiyada", "bank",
    "shaqo", "shaqo abuur", "mushahar",
    "cashuur", "canshuur", "canshuuraha",
    # English (some Somali sites mix English)
    "business", "economy", "economic", "finance", "financial", "investment",
    "market", "import", "export", "trade", "tax", "budget", "revenue", "profit", "loss",
]

# Sources:
# - "type": "wp_category" means WordPress-like /page/N/ pagination
# - "type": "param_page" means ?page=N style pagination
# - "type": "keyword_archive" means general archive + keyword filter to label as Economy
#
# Keep Somali URLs/paths as-is. Only comments/strings here are English.
SOURCES = [
    # Sports (strong)
    {
        "name": "Kooxda",
        "label": "Sports",
        "type": "wp_category",
        "base_url": "https://kooxda.com/category/wararka-ciyaaraha-maanta/",
        "max_pages": 100000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a"],
    },
    # Sports (backup)
    {
        "name": "Goobjoog",
        "label": "Sports",
        "type": "wp_category",
        "base_url": "https://goobjoog.com/qayb/cayaaraha/",
        "max_pages": 100000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a"],
    },

    # Economy (category attempt; if 404 or low yield, script can still finish with keyword sources)
    {
        "name": "Goobjoog",
        "label": "Economy",
        "type": "wp_category",
        "base_url": "https://goobjoog.com/qayb/dhaqaale/",
        "max_pages": 100000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a"],
    },

    # Economy (keyword-based from large archives)
    # These are examples from your successful runs. Add/remove as needed.
    {
        "name": "RadioWaamo",
        "label": "Economy",
        "type": "keyword_archive",
        "base_url": "https://radiowaamo.so/category/wararka/",
        "max_pages": 200000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a", "article h2 a", "article h3 a"],
        "keywords": ECONOMY_KEYWORDS,
    },
    {
        "name": "Hiiraan",
        "label": "Economy",
        "type": "param_page",
        "base_url": "https://www.hiiraan.com/wararkamaanta.php?page=",
        "max_pages": 200000,
        "selectors": ["h1 a", "h2 a", "h3 a", "h4 a"],
        "keywords": ECONOMY_KEYWORDS,
        "start_page": 1,
    },
    {
        "name": "Somalilandtoday",
        "label": "Economy",
        "type": "wp_category",
        "base_url": "https://somalilandtoday.com/category/news/",
        "max_pages": 200000,
        "selectors": ["h1 a", "h2 a", "h3 a", "h4 a", ".entry-title a", ".post-title a"],
        "keywords": ECONOMY_KEYWORDS,
        "use_keyword_filter": True,  # treat as archive; only keep Economy-like titles
    },
]



# Core utilities (DO NOT EDIT)


@dataclass
class AddResult:
    rows: List[dict]
    pages_visited: int
    added: int


def norm_text(s: str) -> str:
    """Normalize text for duplicate detection."""
    return " ".join(str(s).lower().split()).strip()


def wp_page_url(base_url: str, page: int) -> str:
    """
    WordPress-style pagination:
    - page 1: base_url
    - page N: base_url/page/N/
    """
    base = base_url.rstrip("/") + "/"
    return base if page == 1 else f"{base}page/{page}/"


def extract_links(html: str, selectors: List[str]) -> List[Tuple[str, str]]:
    """
    Extract (title, href) pairs from a page using CSS selectors.
    """
    soup = BeautifulSoup(html, "html.parser")
    css = ", ".join(selectors) if selectors else "h1 a, h2 a, h3 a, .entry-title a, .post-title a"
    links = soup.select(css)

    out: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    for a in links:
        title = a.get_text(" ", strip=True)
        href = (a.get("href") or "").strip()

        if not title or len(title) < 12:
            continue

        key = (title, href)
        if key in seen:
            continue
        seen.add(key)
        out.append((title, href))

    return out


def request_page(url: str, headers: dict) -> Optional[requests.Response]:
    """Fetch a URL with common settings and safe error handling."""
    try:
        r = requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS, allow_redirects=True)
        return r
    except requests.RequestException:
        return None


def checkpoint_save(df: pd.DataFrame, out_path: str) -> None:
    """Write checkpoint CSV safely."""
    tmp_path = out_path + ".tmp"
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    os.replace(tmp_path, out_path)


def maybe_sleep():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def collect_from_source(
    source_cfg: dict,
    need: int,
    existing_norm: Set[str],
    headers: dict,
    df_base: pd.DataFrame,
    out_path: str,
) -> AddResult:
    """
    Collect rows from a source until:
    - we reach 'need'
    - or the source ends (404 / empty page)
    - or we hit STOP_AFTER_ZERO_STREAK consecutive pages with zero additions
    """
    name = source_cfg["name"]
    label = source_cfg["label"]
    src_type = source_cfg["type"]
    base_url = source_cfg["base_url"]
    max_pages = int(source_cfg.get("max_pages", 100000))
    selectors = source_cfg.get("selectors", [])
    keywords = source_cfg.get("keywords", [])
    use_keyword_filter = bool(source_cfg.get("use_keyword_filter", False))

    rows: List[dict] = []
    pages_visited = 0
    zero_streak = 0

    # param_page sources can start at page 0 or 1; default 1
    start_page = int(source_cfg.get("start_page", 1))

    for page in range(start_page, max_pages + 1):
        if len(rows) >= need:
            break

        # Build URL according to pagination style
        if src_type == "wp_category" or src_type == "keyword_archive":
            url = wp_page_url(base_url, page)
        elif src_type == "param_page":
            url = f"{base_url}{page}"
        else:
            raise ValueError(f"Unknown source type: {src_type}")

        r = request_page(url, headers)
        if r is None:
            print(f"[{name} - {label}] Page {page}: request error -> continue")
            continue

        if r.status_code != 200:
            print(f"[{name} - {label}] Page {page}: HTTP {r.status_code} -> STOP ({url})")
            break

        items = extract_links(r.text, selectors)
        if not items:
            print(f"[{name} - {label}] Page {page}: 0 items -> STOP ({url})")
            break

        added = 0
        for title, href in items:
            tnorm = norm_text(title)
            if tnorm in existing_norm:
                continue

            # Economy filtering if configured
            if (src_type == "keyword_archive") or use_keyword_filter:
                if keywords and not any(k in tnorm for k in keywords):
                    continue

            existing_norm.add(tnorm)
            rows.append({
                "text": title,
                "label": label,
                "source": name,
                "url": href
            })
            added += 1

            if len(rows) % CHECKPOINT_EVERY == 0:
                df_tmp = pd.concat([df_base, pd.DataFrame(rows)], ignore_index=True)
                checkpoint_save(df_tmp, out_path)
                print(f">> CHECKPOINT SAVED: {out_path} (rows={len(df_tmp)})")

            if len(rows) >= need:
                break

        pages_visited += 1
        print(f"[{name} - {label}] Page {page}: +{added} (total +{len(rows)}/{need})")

        if added == 0:
            zero_streak += 1
        else:
            zero_streak = 0

        if zero_streak >= STOP_AFTER_ZERO_STREAK:
            print(f"[{name} - {label}] {STOP_AFTER_ZERO_STREAK} consecutive pages with 0 additions -> STOP")
            break

        maybe_sleep()

    return AddResult(rows=rows, pages_visited=pages_visited, added=len(rows))


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure required columns exist and clean minimal issues."""
    for col in [TEXT_COL, LABEL_COL]:
        if col not in df.columns:
            raise ValueError(f"Input CSV must contain column '{col}'")

    df = df.dropna(subset=[TEXT_COL, LABEL_COL]).copy()
    df[TEXT_COL] = df[TEXT_COL].astype(str).str.strip()
    df[LABEL_COL] = df[LABEL_COL].astype(str).str.strip()
    df = df[df[TEXT_COL].str.len() > 5].reset_index(drop=True)

    # Optional columns
    if "source" not in df.columns:
        df["source"] = ""
    if "url" not in df.columns:
        df["url"] = ""

    return df


def build_existing_norm(df: pd.DataFrame) -> Set[str]:
    return set(norm_text(t) for t in df[TEXT_COL].tolist())


def print_distribution(df: pd.DataFrame) -> None:
    print(df[LABEL_COL].value_counts())


def labels_needed(df: pd.DataFrame, target_per_label: int) -> Dict[str, int]:
    counts = df[LABEL_COL].value_counts().to_dict()
    want = {}
    for lbl in ["Politics", "World", "Sports", "Economy"]:
        want[lbl] = max(0, target_per_label - counts.get(lbl, 0))
    return want


def main():
    parser = argparse.ArgumentParser(
        description="Build a balanced Somali news classification dataset (headlines)."
    )
    parser.add_argument("--in", dest="in_path", required=True, help="Input CSV path")
    parser.add_argument("--out", dest="out_path", required=True, help="Output CSV path")
    parser.add_argument("--target", dest="target", type=int, default=TARGET_PER_LABEL, help="Target rows per label")
    args = parser.parse_args()

    in_path = args.in_path
    out_path = args.out_path
    target = args.target

    df = pd.read_csv(in_path)
    df = ensure_columns(df)

    print("Current label distribution:")
    print_distribution(df)

    need = labels_needed(df, target)
    print("\nRows needed to reach target per label:")
    for k, v in need.items():
        print(f"- {k}: {v}")

    # Duplicate detection set
    existing_norm = build_existing_norm(df)

    # Use default headers (can be overridden if needed)
    headers = dict(DEFAULT_HEADERS)

    # Collect new rows label by label in a deterministic order:
    # 1) Sports sources
    # 2) Economy sources
    #
    # This makes the script easy to follow and edit.
    new_rows: List[dict] = []

    #  SPORTS 
    if need["Sports"] > 0:
        sports_need = need["Sports"]
        print(f"\n=== Collecting Sports: need {sports_need} ===")

        for src in [s for s in SOURCES if s["label"] == "Sports"]:
            if sports_need <= 0:
                break

            res = collect_from_source(
                source_cfg=src,
                need=sports_need,
                existing_norm=existing_norm,
                headers=headers,
                df_base=df,
                out_path=out_path,
            )
            new_rows.extend(res.rows)
            sports_need -= res.added

        if sports_need > 0:
            print(f"[Sports] Still missing {sports_need}. Add another Sports source to SOURCES.")

    # Update df_base for better checkpoints during Economy
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        checkpoint_save(df, out_path)
        new_rows = []  # reset for next stage
        print(f"\nCheckpoint after Sports saved: {out_path}")

    # Recompute need after Sports
    need = labels_needed(df, target)

    # ECONOMY 
    if need["Economy"] > 0:
        econ_need = need["Economy"]
        print(f"\n=== Collecting Economy: need {econ_need} ===")

        for src in [s for s in SOURCES if s["label"] == "Economy"]:
            if econ_need <= 0:
                break

            res = collect_from_source(
                source_cfg=src,
                need=econ_need,
                existing_norm=existing_norm,
                headers=headers,
                df_base=df,
                out_path=out_path,
            )
            new_rows.extend(res.rows)
            econ_need -= res.added

        if econ_need > 0:
            print(f"[Economy] Still missing {econ_need}. Add another Economy source to SOURCES.")

    # Final write
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # Final de-dup by normalized text (extra safety)
    df["_norm"] = df[TEXT_COL].apply(norm_text)
    df = df.drop_duplicates(subset=["_norm"]).drop(columns=["_norm"]).reset_index(drop=True)

    checkpoint_save(df, out_path)

    print("\nFinal label distribution:")
    print_distribution(df)
    print(f"\nSaved: {out_path}")
    print(f"Total rows: {len(df)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user. If checkpoints were enabled, your output file may still be updated.")
        sys.exit(130)
