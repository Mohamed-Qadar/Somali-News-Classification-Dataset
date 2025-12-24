"""
Core scraping utilities.

This file contains:
- HTTP requests with basic anti-block friendliness (headers + sleep)
- robust title extraction via CSS selectors
- duplicate prevention via normalized text
- checkpoint writing (safe to interrupt)
"""

from __future__ import annotations

import os
import time
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup


@dataclass
class ScrapeConfig:
    target_per_label: int = 5255
    text_col: str = "text"
    label_col: str = "label"
    timeout_seconds: int = 25
    sleep_min: float = 0.5
    sleep_max: float = 1.2
    checkpoint_every: int = 300
    stop_after_zero_streak: int = 80
    min_title_len: int = 12


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


def extract_links(html: str, selectors: List[str], min_len: int) -> List[Tuple[str, str]]:
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

        if not title or len(title) < min_len:
            continue

        key = (title, href)
        if key in seen:
            continue

        seen.add(key)
        out.append((title, href))

    return out


def request_page(url: str, headers: Dict[str, str], timeout: int) -> Optional[requests.Response]:
    """Fetch a page safely; return None on network errors."""
    try:
        return requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    except requests.RequestException:
        return None


def safe_write_csv(df: pd.DataFrame, out_path: str) -> None:
    """
    Write CSV using atomic replace to reduce corruption risk if interrupted.
    """
    tmp_path = out_path + ".tmp"
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    os.replace(tmp_path, out_path)


def ensure_columns(df: pd.DataFrame, text_col: str, label_col: str) -> pd.DataFrame:
    """Ensure required columns exist and minimal cleaning is applied."""
    if text_col not in df.columns or label_col not in df.columns:
        raise ValueError(f"Input CSV must contain '{text_col}' and '{label_col}' columns")

    df = df.dropna(subset=[text_col, label_col]).copy()
    df[text_col] = df[text_col].astype(str).str.strip()
    df[label_col] = df[label_col].astype(str).str.strip()
    df = df[df[text_col].str.len() > 5].reset_index(drop=True)

    # Optional metadata columns
    if "source" not in df.columns:
        df["source"] = ""
    if "url" not in df.columns:
        df["url"] = ""

    return df


def label_needs(df: pd.DataFrame, label_col: str, target: int, labels: List[str]) -> Dict[str, int]:
    counts = df[label_col].value_counts().to_dict()
    return {lbl: max(0, target - counts.get(lbl, 0)) for lbl in labels}


def maybe_sleep(cfg: ScrapeConfig) -> None:
    time.sleep(random.uniform(cfg.sleep_min, cfg.sleep_max))


def collect_from_source(
    source: dict,
    need: int,
    existing_norm: Set[str],
    headers: Dict[str, str],
    cfg: ScrapeConfig,
    df_base: pd.DataFrame,
    out_path: str,
) -> List[dict]:
    """
    Collect up to 'need' rows from a source.
    Stops on:
    - non-200 HTTP status
    - empty page (no extracted items)
    - too many consecutive pages with zero additions
    """
    name = source["name"]
    label = source["label"]
    src_type = source["type"]
    base_url = source["base_url"]
    max_pages = int(source.get("max_pages", 100000))
    selectors = source.get("selectors", [])
    keywords = source.get("keywords", [])
    pagination = source.get("pagination", "wp")  # "wp" or "param"
    start_page = int(source.get("start_page", 1))

    rows: List[dict] = []
    zero_streak = 0

    for page in range(start_page, max_pages + 1):
        if len(rows) >= need:
            break

        if pagination == "wp":
            url = wp_page_url(base_url, page)
        elif pagination == "param":
            url = f"{base_url}{page}"
        else:
            raise ValueError(f"Unknown pagination mode: {pagination}")

        r = request_page(url, headers, cfg.timeout_seconds)
        if r is None:
            print(f"[{name} - {label}] Page {page}: request error -> continue")
            continue

        if r.status_code != 200:
            print(f"[{name} - {label}] Page {page}: HTTP {r.status_code} -> STOP ({url})")
            break

        items = extract_links(r.text, selectors, cfg.min_title_len)
        if not items:
            print(f"[{name} - {label}] Page {page}: 0 items -> STOP ({url})")
            break

        added = 0
        for title, href in items:
            tnorm = norm_text(title)
            if tnorm in existing_norm:
                continue

            # Keyword filter for keyword_archive sources
            if src_type == "keyword_archive":
                if keywords and not any(k in tnorm for k in keywords):
                    continue

            existing_norm.add(tnorm)
            rows.append({"text": title, "label": label, "source": name, "url": href})
            added += 1

            if len(rows) % cfg.checkpoint_every == 0:
                tmp = pd.concat([df_base, pd.DataFrame(rows)], ignore_index=True)
                safe_write_csv(tmp, out_path)
                print(f">> CHECKPOINT SAVED: {out_path} (rows={len(tmp)})")

            if len(rows) >= need:
                break

        print(f"[{name} - {label}] Page {page}: +{added} (total +{len(rows)}/{need})")

        if added == 0:
            zero_streak += 1
        else:
            zero_streak = 0

        if zero_streak >= cfg.stop_after_zero_streak:
            print(f"[{name} - {label}] {cfg.stop_after_zero_streak} pages with 0 additions -> STOP")
            break

        maybe_sleep(cfg)

    return rows
