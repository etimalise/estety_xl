import pandas as pd
from typing import Iterable, Tuple, Dict, Any

def dedupe_by_timestamp(
    df: pd.DataFrame,
    *,
    time_col: str = "Timestamp",
    ignore_cols: Iterable[str] = ("_src_row",),
    keep: str = "last",   # or "first"
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Deduplicate by comparing all columns except `time_col` and `ignore_cols`.
    Keep earliest or latest by `time_col`.
    Returns: (deduped_df, report_df, removed_json)
    """
    if time_col not in df.columns:
        raise KeyError(f"Time column '{time_col}' not found")

    out = df.copy()
    out[time_col] = pd.to_datetime(out[time_col], errors="coerce")

    subset_cols = [c for c in out.columns if c not in {time_col, *ignore_cols}]

    asc = (keep == "first")
    out = out.sort_values(time_col, ascending=asc)

    dup_mask = out.duplicated(subset=subset_cols, keep="last" if keep == "last" else "first")
    removed = out.loc[dup_mask].copy()

    deduped = out.drop_duplicates(subset=subset_cols, keep="last" if keep == "last" else "first")

    # Report table
    sig = out[subset_cols].astype(str).agg("§".join, axis=1)
    out["_sig"] = sig
    kept_mask = ~dup_mask
    kept = out.loc[kept_mask].drop_duplicates("_sig", keep="last" if keep == "last" else "first")

    report = (
        removed.assign(_sig=sig[dup_mask])
        .groupby("_sig")
        .agg(
            removed_rows=("Timestamp", "count"),
            removed_min_ts=("Timestamp", "min"),
            removed_max_ts=("Timestamp", "max"),
        )
        .join(
            kept.set_index("_sig")[[time_col]].rename(columns={time_col: "kept_ts"}),
            how="right"
        )
        .fillna({"removed_rows": 0})
        .reset_index(drop=True)
    )

    # JSON dump of removed duplicates
    removed_json = removed.to_dict(orient="records")

    deduped = deduped.drop(columns=["_sig"], errors="ignore")

    return deduped, report, removed_json
