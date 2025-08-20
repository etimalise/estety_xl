from __future__ import annotations
from typing import Tuple
from pathlib import Path
import json
from datetime import datetime
import pandas as pd
from utils import (
    dotenv as env, secrets as sec,
    get_filename)
from add_child import normalize_children
from de_dup import dedupe_by_timestamp
from analysis import analyze_similarity

def load_files() -> Tuple[pd.DataFrame, pd.DataFrame, Path] | None:
    ### loading files into datasets, if no output file, create new
    def pick_sheet(file):
        print(f"Loading:\t[{file}]")
        xls = pd.ExcelFile(file)
        sheets = xls.sheet_names
        menu = " ".join(f"\033[93m{i + 1}\033[0m.{s}" for i, s in enumerate(sheets))
        if len(sheets) > 1:
            choice = int(input(f"Pick sheet [{menu}]: "))
            if choice > len(sheets):
                print(f"Invalid sheet selection [{choice}]")
                return None
            print(f"Pick sheet:\t[{sheets[choice-1]}]")
            return sheets[choice - 1]
        else:
            print(f"Pick sheet:\t[{sheets[0]}]")
            return sheets[0]

    infile = get_filename(env.import_file)
    outfile = get_filename(env.export_file)
    if not infile:
        print("Import file not found")
        return None
    sheet_name = pick_sheet(infile)
    if not sheet_name: return None
    pd_in = pd.read_excel(infile, sheet_name=sheet_name)

    if not outfile:
        print("Export file not found, generating new file")
        outfile = env.rootpath / Path(env.export_file)
        pd_out = pd.DataFrame()
    else:
        sheet_name = pick_sheet(outfile)
        if not sheet_name: return None
        pd_out = pd.read_excel(outfile, sheet_name=sheet_name)

    #print("Import file: {}".format(infile))
    #print("Export file: {}".format(outfile))
    print()
    return pd_in, pd_out, outfile


def main() -> int:
    print("\nExcel parser: import, normalize, de-duplicate, optimize, export\n")

    ### checking files if exist
    files = load_files()
    if not files: return 404
    df_in, df_out, outfile = files

    ### working with datasets - normalize children, add second row with second child, rename column to no-number
    norm = normalize_children(df_in)
    deduped, report, removed_json = dedupe_by_timestamp(norm)
    print("\nRemoved records:", removed_json[:2])  # preview

    selected_cols = [
        "Фото- и видеосъёмка",
        "Фото- и видеосъемка",
        "Обработка персональных данных",
        "Вступление в объединение",
        "Добавить следующего ребенка?",
        "_src_row"
    ]

    ### analysis of similarities across rows
    groups, report = analyze_similarity(
        deduped,
        ignore_cols=selected_cols,
        min_group_size=2,
        min_similarity_pct=20.0   # only groups where some pair is ≥20% similar
    )
    #print(json.dumps(groups, ensure_ascii=False, indent=2))
    #print(report)
    print()

    ### removing excessive rows
    optimized_df = deduped.drop(columns=[c for c in selected_cols if c in deduped.columns], errors="ignore")
    print(optimized_df.info())


    ### compare and add to master file



    ### saving results into master xl file

    sheet_name = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\nWriting sheet {sheet_name} into {outfile}")
    try:
        # append if file exists
        with pd.ExcelWriter(outfile, mode="a", engine="openpyxl", if_sheet_exists="new") as writer:
            optimized_df.to_excel(writer, sheet_name=sheet_name, index=False)
    except FileNotFoundError:
        # create new file
        with pd.ExcelWriter(outfile, mode="w", engine="openpyxl") as writer:
            optimized_df.to_excel(writer, sheet_name=sheet_name, index=False)
    return 0


if __name__ == "__main__":
    main()
