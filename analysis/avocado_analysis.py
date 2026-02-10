from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "avocado.csv"
JSON_PATH = BASE_DIR / "avocado.json"
DB_PATH = BASE_DIR / "avocado.sqlite"
TABLE_NAME = "avocado"
CHART_PATH = BASE_DIR / "avg_price_by_type.png"
STATS_PATH = BASE_DIR / "category_numeric_stats.csv"
CONCLUSIONS_PATH = BASE_DIR / "conclusions.md"
CACHE_DIR = BASE_DIR / ".cache"

# Keep matplotlib cache within the workspace to avoid permission issues.
CACHE_DIR.mkdir(exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(CACHE_DIR))
os.environ.setdefault("XDG_CACHE_HOME", str(CACHE_DIR))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def convert_csv_to_json(csv_path: Path, json_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    records = df.to_dict(orient="records")
    json_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    return df


def load_json_to_sqlite(json_path: Path, db_path: Path, table_name: str) -> None:
    records = json.loads(json_path.read_text(encoding="utf-8"))
    df = pd.DataFrame(records)

    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)


def fetch_from_sqlite(db_path: Path, table_name: str) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql_query(query, conn)


def remove_outliers_iqr(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    filtered = df.copy()

    for col in numeric_cols:
        q1 = filtered[col].quantile(0.25)
        q3 = filtered[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        filtered = filtered[filtered[col].between(lower_bound, upper_bound)]

    return filtered


def clean_data(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    work = df.copy()

    if "Unnamed: 0" in work.columns:
        work = work.drop(columns=["Unnamed: 0"])

    work["Date"] = pd.to_datetime(work["Date"], errors="coerce")

    before_dups = len(work)
    work = work.drop_duplicates()
    after_dups = len(work)

    numeric_cols = work.select_dtypes(include=["number"]).columns.tolist()
    numeric_cols = [c for c in numeric_cols if c != "year"]

    before_outliers = len(work)
    work = remove_outliers_iqr(work, numeric_cols)
    after_outliers = len(work)

    report = {
        "rows_before": len(df),
        "rows_after_drop_duplicates": after_dups,
        "duplicates_removed": before_dups - after_dups,
        "rows_after_outlier_filter": after_outliers,
        "outliers_removed": before_outliers - after_outliers,
    }
    return work, report


def compute_category_stats(df: pd.DataFrame, category_col: str = "type") -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    agg_spec = {col: ["count", "mean", "median", "min", "max", "std"] for col in numeric_cols}
    stats = df.groupby(category_col).agg(agg_spec)

    # Flatten MultiIndex columns for convenient export
    stats.columns = [f"{col}_{metric}" for col, metric in stats.columns]
    return stats.reset_index()


def plot_category_chart(df: pd.DataFrame, chart_path: Path) -> None:
    avg_price = (
        df.groupby("type", as_index=False)["AveragePrice"]
        .mean()
        .sort_values("AveragePrice", ascending=False)
    )

    plt.figure(figsize=(8, 5))
    plt.bar(avg_price["type"], avg_price["AveragePrice"], color=["#2d7f5e", "#8abf26"])
    plt.title("Average avocado price by type (cleaned data)")
    plt.xlabel("Type")
    plt.ylabel("AveragePrice")
    plt.tight_layout()
    plt.savefig(chart_path, dpi=140)
    plt.close()


def build_conclusions(report: dict[str, int], stats: pd.DataFrame) -> str:
    stats_idx = stats.set_index("type")
    conventional_price = float(stats_idx.loc["conventional", "AveragePrice_mean"])
    organic_price = float(stats_idx.loc["organic", "AveragePrice_mean"])
    conventional_volume = float(stats_idx.loc["conventional", "Total Volume_mean"])
    organic_volume = float(stats_idx.loc["organic", "Total Volume_mean"])

    return (
        "# Выводы по анализу avocado\n\n"
        f"- Исходный объем данных: **{report['rows_before']}** строк.\n"
        f"- Удалено дубликатов: **{report['duplicates_removed']}**.\n"
        f"- После фильтрации выбросов осталось: **{report['rows_after_outlier_filter']}** строк "
        f"(удалено **{report['outliers_removed']}**).\n"
        f"- Средняя цена `organic`: **{organic_price:.3f}**, `conventional`: **{conventional_price:.3f}**.\n"
        f"- `organic` в среднем дороже на **{organic_price - conventional_price:.3f}**.\n"
        f"- Средний объем продаж `conventional`: **{conventional_volume:,.2f}**, "
        f"`organic`: **{organic_volume:,.2f}**.\n"
        "- По очищенным данным сегмент `conventional` продается в больших объемах, "
        "а `organic` стабильно дороже.\n"
    )


def main() -> None:
    convert_csv_to_json(CSV_PATH, JSON_PATH)
    load_json_to_sqlite(JSON_PATH, DB_PATH, TABLE_NAME)

    raw_df = fetch_from_sqlite(DB_PATH, TABLE_NAME)
    clean_df, report = clean_data(raw_df)

    stats = compute_category_stats(clean_df, category_col="type")
    stats.to_csv(STATS_PATH, index=False)

    plot_category_chart(clean_df, CHART_PATH)
    conclusions = build_conclusions(report, stats)
    CONCLUSIONS_PATH.write_text(conclusions, encoding="utf-8")

    print("=== PIPELINE REPORT ===")
    for key, value in report.items():
        print(f"{key}: {value}")

    print("\n=== CATEGORY STATS (type) ===")
    print(stats.to_string(index=False))

    print("\nSaved files:")
    print(f"- JSON: {JSON_PATH}")
    print(f"- SQLite DB: {DB_PATH}")
    print(f"- Stats CSV: {STATS_PATH}")
    print(f"- Chart: {CHART_PATH}")
    print(f"- Conclusions: {CONCLUSIONS_PATH}")


if __name__ == "__main__":
    main()
