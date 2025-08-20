# analysis.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
import itertools
import pandas as pd
import numpy as np

def _norm(v: Any) -> Any:
    if pd.isna(v): return None
    s = str(v).strip()
    if s == "" or s == "Нет" or s == "нет": return None
    return s.lower()

def analyze_similarity(
    df: pd.DataFrame,
    ignore_cols: List[str] | None = None,
    min_group_size: int = 2,
    min_similarity_pct: float = 0.0
) -> Tuple[List[Dict[str, Any]], pd.DataFrame]:
    """
    Groups rows that share at least one common (non-NaN, non-blank) field value,
    excluding ignore_cols. Only emits groups where common fields exist and pass
    the min_similarity_pct cutoff.
    """
    ignore = set(ignore_cols or [])
    cols = [c for c in df.columns if c not in ignore]

    groups_map: Dict[frozenset, Dict[str, Any]] = {}
    for col in cols:
        series = df[col].apply(_norm)
        buckets: Dict[Any, List[int]] = {}
        for idx, val in series.items():
            if val is None:
                continue
            buckets.setdefault(val, []).append(idx)

        for norm_val, idxs in buckets.items():
            if len(idxs) < min_group_size:
                continue
            key = frozenset(idxs)
            rec = groups_map.setdefault(key, {"rows": sorted(idxs), "common": {}})
            rep = df.loc[idxs[0], col]
            rec["common"][col] = rep

    groups: List[Dict[str, Any]] = []
    sim_rows = []

    for gid, (_, rec) in enumerate(groups_map.items(), start=1):
        if not rec["common"]:
            continue

        rows = rec["rows"]
        common = rec["common"].copy()

        # Remove 'common' keys where at least one row has NaN/blank
        to_delete = []
        for col in list(common):
            vals = df.loc[rows, col]
            if any(_norm(v) is None for v in vals):
                to_delete.append(col)
        for col in to_delete:
            del common[col]

        if not common:
            continue

        # Build differences
        diffs = []
        diff_cols = [c for c in cols if c not in common]
        for r in rows:
            entry = {"row": int(r)}
            for c in diff_cols:
                val = df.at[r, c]
                if _norm(val) is None:
                    continue
                entry[c] = val
            diffs.append(entry)

        # Calculate pairwise similarities
        local_sims = []
        for a, b in itertools.combinations(rows, 2):
            matches = 0
            denom = 0
            for c in cols:
                va, vb = df.at[a, c], df.at[b, c]
                na, nb = _norm(va), _norm(vb)
                if na is None or nb is None:
                    continue
                denom += 1
                if na == nb:
                    matches += 1
            if denom:
                pct = round(100.0 * matches / denom, 2)
                local_sims.append(pct)
                sim_rows.append({
                    "group": gid,
                    "row_a": int(a),
                    "row_b": int(b),
                    "similarity_%": pct
                })

        # Skip group if no pair passes cutoff
        if min_similarity_pct > 0 and not any(p >= min_similarity_pct for p in local_sims):
            continue

        groups.append({
            "group_id": gid,
            "rows": rows,
            "common": common,
            "differences": diffs
        })

    sim_df = pd.DataFrame(sim_rows) if sim_rows else pd.DataFrame(columns=["group","row_a","row_b","similarity_%"])
    return groups, sim_df
