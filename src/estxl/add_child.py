import pandas as pd
import re, unicodedata
from typing import Iterable

# --- config ---
BASES: list[str] = [
    "Имя и фамилия ребенка (SE)",
    "Имя и фамилия ребенка (RU)",
    "Дата рождения ребенка",
    "Аллергии и особенности ребенка",
]
KEY_BASE = "Имя и фамилия ребенка (SE)"  # used to detect presence of a child
# -------------

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("ё", "е").replace("Ё", "Е")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _pair(base: str) -> tuple[str, str]:
    return f"{base} #1", f"{base} #2"

def _nonempty(series: pd.Series) -> pd.Series:
    return series.notna() & (series.astype(str).str.strip() != "")

def normalize_children(
    df: pd.DataFrame,
    *,
    bases: Iterable[str] = BASES,
    key_base: str = KEY_BASE,
    keep_original: bool = False,
    strict_check: bool = True,
) -> pd.DataFrame:
    """Duplicate rows to 1-per-child, unify #1/#2 into base columns, and self-validate."""
    # 0) normalize headers; keep source row id for validation
    df = df.rename(columns=lambda c: _norm(str(c))).copy()
    df["_src_row"] = range(len(df))

    # 1) child #1 view: rename "#1" -> base
    df1 = df.copy()
    rename_1 = {a: base for base in bases if (a := _pair(base)[0]) in df.columns}
    df1 = df1.rename(columns=rename_1)

    # 2) child #2 view: overwrite base from "#2" (if exists)
    df2 = df.copy()
    for base in bases:
        a, b = _pair(base)
        if b in df2.columns:
            df2[base] = df2[b]
        elif base not in df2.columns:
            df2[base] = pd.NA

    # 3) filter: keep only rows where the respective child exists
    #    (#1 present if either base exists and nonempty OR "#1" exists and nonempty)
    def has_child(d: pd.DataFrame, base_name: str, which: int) -> pd.Series:
        a, b = _pair(base_name)
        if which == 1:
            if base_name in d.columns:
                return _nonempty(d[base_name])
            if a in d.columns:
                return _nonempty(d[a])
            return pd.Series(False, index=d.index)
        else:
            return _nonempty(d[b]) if b in d.columns else pd.Series(False, index=d.index)

    mask1 = has_child(df1, key_base, which=1)
    mask2 = has_child(df2, key_base, which=2)
    df1 = df1[mask1].copy()
    df2 = df2[mask2].copy()

    # 4) concatenate; drop original #1/#2 columns if requested
    out = pd.concat([df1, df2], ignore_index=True)
    if not keep_original:
        drop_cols = [c for base in bases for c in _pair(base)]
        out = out.drop(columns=[c for c in drop_cols if c in out.columns], errors="ignore")

    # -------------------
    # 5) SELF‑CHECK block
    # -------------------
    # For every source row that had a nonempty "#2" KEY_BASE, ensure there exists
    # at least one output row with the same _src_row where all base columns equal the original #2 values.
    b_key_1, b_key_2 = _pair(key_base)
    # Identify source rows with a real #2 child
    has_second_rows = (
        (b_key_2 in df.columns) and _nonempty(df[b_key_2])
    )
    if isinstance(has_second_rows, pd.Series):
        src_need = df.loc[has_second_rows, ["_src_row"]].copy()
        # Build the expected #2 child slice with unified base columns
        expected_cols = ["_src_row"] + list(bases)
        expected = pd.DataFrame(index=src_need.index)
        expected["_src_row"] = df.loc[src_need.index, "_src_row"].values
        for base in bases:
            a, b = _pair(base)
            # take value from "#2" if present; otherwise NaN (won't be checked as required)
            expected[base] = df[b] if b in df.columns else pd.NA

        # Now verify: for each _src_row, does out contain a row matching these base values?
        failures = []
        # Build an index on out for speed
        out_by_src = out.set_index("_src_row")
        for ridx, row in expected.iterrows():
            srow = row["_src_row"]
            if srow not in out_by_src.index:
                failures.append(int(srow))
                continue
            cand = out_by_src.loc[[srow]] if isinstance(out_by_src.loc[srow], pd.Series) else out_by_src.loc[srow]
            # compare only on columns where expected value is non-null
            cols_check = [c for c in bases if pd.notna(row[c])]
            if cols_check:
                ok = (cand[cols_check] == row[cols_check].values).all(axis=1).any()
            else:
                ok = True  # nothing to check for this row
            if not ok:
                failures.append(int(srow))

        if strict_check and failures:
            raise AssertionError(
                f"Normalization self-check failed for source rows (0-based): {sorted(failures)}. "
                f"Values from '#2' were not found in unified output for these rows."
            )

    return out
