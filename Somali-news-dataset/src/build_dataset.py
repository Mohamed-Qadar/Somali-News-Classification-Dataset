#!/usr/bin/env python3
"""
Somali News Dataset Builder (Balanced, Reproducible)

This script extends an existing CSV until each label reaches TARGET_PER_LABEL.

Design goals:
- One command to run
- Easy to add new sources (edit sources.py only)
- Checkpointing (safe to interrupt)
- Clear logging for debugging

Example:
  python src/build_dataset.py --in "/content/base.csv" --out "/content/final.csv" --target 5255
"""

from __future__ import annotations

import argparse
from typing import List, Dict, Set

import pandas as pd

from scraper_core import (
    ScrapeConfig,
    ensure_columns,
    label_needs,
    norm_text,
    safe_write_csv,
    collect_from_source,
)
from sources import SOURCES


LABELS_DEFAULT = ["Politics", "World", "Sports", "Economy"]


def build_existing_norm(df: pd.DataFrame, text_col: str) -> Set[str]:
    return set(norm_text(t) for t in df[text_col].tolist())


def print_distribution(df: pd.DataFrame, label_col: str) -> None:
    print(df[label_col].value_counts())


def main():
    parser = argparse.ArgumentParser(description="Build a balanced Somali news classification dataset (headlines).")
    parser.add_argument("--in", dest="in_path", required=True, help="Input CSV path (existing dataset)")
    parser.add_argument("--out", dest="out_path", required=True, help="Output CSV path (checkpoint + final)")
    parser.add_argument("--target", dest="target", type=int, default=5255, help="Target rows per label")
    parser.add_argument(
        "--labels",
        dest="labels",
        default=",".join(LABELS_DEFAULT),
        help="Comma-separated labels to balance (default: Politics,World,Sports,Economy)",
    )
    args = parser.parse_args()

    labels: List[str] = [x.strip() for x in args.labels.split(",") if x.strip()]

    cfg = ScrapeConfig(target_per_label=args.target)

    df = pd.read_csv(args.in_path)
    df = ensure_columns(df, cfg.text_col, cfg.label_col)

    print("Current label distribution:")
    print_distribution(df, cfg.label_col)

    need_map = label_needs(df, cfg.label_col, cfg.target_per_label, labels)
    print("\nRows needed:")
    for k, v in need_map.items():
        print(f"- {k}: {v}")

    existing_norm = build_existing_norm(df, cfg.text_col)

    # Collect label by label (deterministic, easier to follow)
    for label in labels:
        need = need_map.get(label, 0)
        if need <= 0:
            continue

        print(f"\n=== Collecting '{label}' (need {need}) ===")

        # Sources that match this label
        candidates = [s for s in SOURCES if s["label"] == label]
        if not candidates:
            print(f"No sources defined for label '{label}'. Add to SOURCES in src/sources.py.")
            continue

        added_rows = []
        remaining = need

        for src in candidates:
            if remaining <= 0:
                break

            new_rows = collect_from_source(
                source=src,
                need=remaining,
                existing_norm=existing_norm,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
                cfg=cfg,
                df_base=df,
                out_path=args.out_path,
            )

            added_rows.extend(new_rows)
            remaining -= len(new_rows)

        if added_rows:
            df = pd.concat([df, pd.DataFrame(added_rows)], ignore_index=True)
            safe_write_csv(df, args.out_path)
            print(f"Checkpoint after '{label}' saved: {args.out_path}")

        if remaining > 0:
            print(f"Label '{label}' still missing {remaining}. Add more sources or increase max_pages/selectors.")

    # Final cleanup: de-dup once more by normalized title (extra safety)
    df["_norm"] = df[cfg.text_col].apply(norm_text)
    df = df.drop_duplicates(subset=["_norm"]).drop(columns=["_norm"]).reset_index(drop=True)

    safe_write_csv(df, args.out_path)

    print("\nFinal label distribution:")
    print_distribution(df, cfg.label_col)
    print(f"\nSaved: {args.out_path}")
    print(f"Total rows: {len(df)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user. Output file may still contain the latest checkpoint.")
        raise
