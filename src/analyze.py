from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import csv
import math
import xml.etree.ElementTree as ET
import zipfile
from typing import Any

try:
    import pandas as pd  # type: ignore
except ImportError:  # pragma: no cover - fallback for minimal environments
    pd = None

MISSING_MARKERS = {"", "na", "n/a", "null", "none", "-"}
INPUT_PATH = Path("data/Grad Program Exit Survey Data 2024.xlsx")
OUTPUT_DIR = Path("outputs")

CORE_COURSE_MAP = {
    "Q35_1": "ACC 6060 Professionalism and Leadership",
    "Q35_2": "ACC 6400 Advanced Tax Business Entities",
    "Q35_3": "ACC 6540 Professional Ethics",
    "Q35_4": "ACC 6510 Financial Audit",
    "Q35_5": "ACC 6300 Data Analytics",
    "Q35_8": "ACC 6560 Financial Theory & Research I",
    "Q35_9": "ACC 6350 Management Control Systems",
    "Q35_10": "ACC 6600 Business Law for Accountants",
}

RATING_COURSE_MAP = {
    "Q76_1": "ACC 6020",
    "Q77_2": "ACC 6140",
    "Q78_3": "ACC 6150",
    "Q83_4": "ACC 6250",
    "Q82_5": "ACC 6350",
    "Q80_6": "ACC 6410",
    "Q81_9": "ACC 6600",
    "Q79_7": "ACC 679R",
}


@dataclass
class Table:
    columns: list[str]
    rows: list[dict[str, object]]



def _clean_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed.lower() in MISSING_MARKERS:
            return None
        return trimmed
    return value



def _to_number(value: object) -> float | None:
    cleaned = _clean_value(value)
    if cleaned is None:
        return None
    if isinstance(cleaned, (int, float)):
        return float(cleaned)
    try:
        return float(str(cleaned).strip())
    except (TypeError, ValueError):
        return None



def _load_data_with_pandas(path: Path) -> Table:
    df = pd.read_excel(path, dtype=object, na_values=["", "NA", "N/A", "null", "None", "-"])
    df = df.iloc[2:].reset_index(drop=True)
    for col in df.columns:
        df[col] = df[col].map(_clean_value)

    rows = df.to_dict(orient="records")
    return Table(columns=[str(c) for c in df.columns.tolist()], rows=rows)



def _xlsx_cell_to_text(cell: ET.Element, shared_strings: list[str], ns: str) -> str | None:
    t_attr = cell.attrib.get("t")
    value_node = cell.find(f"{ns}v")
    if value_node is None or value_node.text is None:
        return None
    value = value_node.text
    if t_attr == "s":
        return shared_strings[int(value)]
    return value



def _parse_excel_without_pandas(path: Path) -> Table:
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with zipfile.ZipFile(path) as archive:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            ss_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for si in ss_root.findall(f"{ns}si"):
                text = "".join(t.text or "" for t in si.iter(f"{ns}t"))
                shared_strings.append(text)

        sheet_root = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
        rows_xml = sheet_root.find(f"{ns}sheetData")
        if rows_xml is None:
            return Table(columns=[], rows=[])

        parsed_rows: list[list[str | None]] = []
        max_cols = 0
        for row in rows_xml.findall(f"{ns}row"):
            row_vals: dict[int, str | None] = {}
            for cell in row.findall(f"{ns}c"):
                ref = cell.attrib.get("r", "")
                letters = "".join(ch for ch in ref if ch.isalpha())
                idx = 0
                for ch in letters:
                    idx = idx * 26 + (ord(ch.upper()) - 64)
                idx -= 1
                row_vals[idx] = _xlsx_cell_to_text(cell, shared_strings, ns)
            if row_vals:
                current_max = max(row_vals.keys()) + 1
                max_cols = max(max_cols, current_max)
                parsed_rows.append([row_vals.get(i) for i in range(current_max)])

        if not parsed_rows:
            return Table(columns=[], rows=[])

        normalized = [r + [None] * (max_cols - len(r)) for r in parsed_rows]
        headers = [str(h).strip() if h is not None else f"col_{i+1}" for i, h in enumerate(normalized[0])]
        data_rows = normalized[1:]
        data_rows = data_rows[2:] if len(data_rows) >= 2 else []

        cleaned_rows: list[dict[str, object]] = []
        for row in data_rows:
            row_dict: dict[str, object] = {}
            for idx, col in enumerate(headers):
                row_dict[col] = _clean_value(row[idx] if idx < len(row) else None)
            cleaned_rows.append(row_dict)

        return Table(columns=headers, rows=cleaned_rows)



def load_data(path: Path = INPUT_PATH) -> tuple[Table, dict[str, object]]:
    if pd is not None:
        table = _load_data_with_pandas(path)
        loader = "pandas"
    else:
        table = _parse_excel_without_pandas(path)
        loader = "xml_fallback"

    non_empty_rows = []
    for row in table.rows:
        if any(v is not None for v in row.values()):
            non_empty_rows.append(row)
    table.rows = non_empty_rows

    meta = {
        "loader": loader,
        "row_count": len(table.rows),
        "col_count": len(table.columns),
    }
    return table, meta



def _build_ranking(table: Table, column_map: dict[str, str], higher_is_better: bool) -> tuple[list[dict[str, object]], list[str], list[str]]:
    found = [col for col in column_map if col in table.columns]
    missing = [col for col in column_map if col not in table.columns]

    rows: list[dict[str, object]] = []
    for col in found:
        values: list[float] = []
        for row in table.rows:
            numeric = _to_number(row.get(col))
            if numeric is not None:
                values.append(numeric)
        n = len(values)
        score = (sum(values) / n) if n else math.nan
        rows.append({"course": column_map[col], "score": score, "n": n})

    sortable = [r for r in rows if not math.isnan(float(r["score"]))]
    if higher_is_better:
        sortable.sort(key=lambda r: (-float(r["score"]), -int(r["n"]), str(r["course"])))
    else:
        sortable.sort(key=lambda r: (float(r["score"]), -int(r["n"]), str(r["course"])))

    for idx, row in enumerate(sortable, start=1):
        row["rank"] = idx

    return sortable, found, missing



def build_core_rank_ranking(table: Table) -> tuple[list[dict[str, object]], list[str], list[str]]:
    return _build_ranking(table, CORE_COURSE_MAP, higher_is_better=False)



def build_rating_ranking(table: Table) -> tuple[list[dict[str, object]], list[str], list[str]]:
    return _build_ranking(table, RATING_COURSE_MAP, higher_is_better=True)



def _write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            out = {k: row.get(k, "") for k in columns}
            if isinstance(out.get("score"), float):
                out["score"] = f"{out['score']:.6f}"
            writer.writerow(out)



def _markdown_table(rows: list[dict[str, object]], columns: list[str]) -> str:
    if not rows:
        return "_No ranked rows available._"
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    body_lines = []
    for row in rows:
        vals = []
        for col in columns:
            value = row.get(col, "")
            if isinstance(value, float):
                value = f"{value:.6f}"
            vals.append(str(value))
        body_lines.append("| " + " | ".join(vals) + " |")
    return "\n".join([header, divider, *body_lines])



def write_outputs(
    core_rows: list[dict[str, object]],
    rating_rows: list[dict[str, object]],
    core_found: list[str],
    core_missing: list[str],
    rating_found: list[str],
    rating_missing: list[str],
    meta: dict[str, object],
    input_path: Path = INPUT_PATH,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    core_path = output_dir / "core_course_ranking.csv"
    rating_path = output_dir / "rated_course_ranking.csv"
    combined_path = output_dir / "ranking.csv"
    report_path = output_dir / "report.md"

    ranking_cols = ["course", "score", "n", "rank"]
    _write_csv(core_path, core_rows, ranking_cols)
    _write_csv(rating_path, rating_rows, ranking_cols)

    combined_rows: list[dict[str, object]] = []
    for row in core_rows:
        combined_rows.append({
            "course": row["course"],
            "rank_type": "core_rank",
            "score": row["score"],
            "n": row["n"],
            "rank": row["rank"],
        })
    for row in rating_rows:
        combined_rows.append({
            "course": row["course"],
            "rank_type": "rating",
            "score": row["score"],
            "n": row["n"],
            "rank": row["rank"],
        })

    _write_csv(combined_path, combined_rows, ["course", "rank_type", "score", "n", "rank"])

    ts = datetime.now(timezone.utc).isoformat()
    report_lines = [
        "# Ranking Workflow Report",
        "",
        f"- **timestamp (UTC):** {ts}",
        f"- **input path:** `{input_path}`",
        f"- **loader:** `{meta.get('loader')}`",
        f"- **rows after response skip/filter:** {meta.get('row_count', 0)}",
        f"- **columns loaded:** {meta.get('col_count', 0)}",
        "",
        "## Column Availability",
        "",
        f"- **Q35 found:** {', '.join(core_found) if core_found else '(none)'}",
        f"- **Q35 missing:** {', '.join(core_missing) if core_missing else '(none)'}",
        f"- **Rating found:** {', '.join(rating_found) if rating_found else '(none)'}",
        f"- **Rating missing:** {', '.join(rating_missing) if rating_missing else '(none)'}",
        "",
    ]

    if not core_found and not rating_found:
        report_lines.extend([
            "## Notes",
            "",
            "Both ranking groups were fully missing. Empty CSV files were generated with headers only.",
            "",
        ])

    report_lines.extend([
        "## Core Course Ranking (Top 10)",
        "",
        _markdown_table(core_rows[:10], ["rank", "course", "score", "n"]),
        "",
        "## Rated Course Ranking (Top 10)",
        "",
        _markdown_table(rating_rows[:10], ["rank", "course", "score", "n"]),
        "",
    ])

    report_path.write_text("\n".join(report_lines), encoding="utf-8")


def write_failure_outputs(
    *,
    message: str,
    input_path: Path = INPUT_PATH,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    """Write empty output artifacts with a clear failure report."""
    output_dir.mkdir(parents=True, exist_ok=True)
    empty_rows: list[dict[str, Any]] = []

    _write_csv(output_dir / "core_course_ranking.csv", empty_rows, ["course", "score", "n", "rank"])
    _write_csv(output_dir / "rated_course_ranking.csv", empty_rows, ["course", "score", "n", "rank"])
    _write_csv(output_dir / "ranking.csv", empty_rows, ["course", "rank_type", "score", "n", "rank"])

    ts = datetime.now(timezone.utc).isoformat()
    report = "\n".join(
        [
            "# Ranking Workflow Report",
            "",
            f"- **timestamp (UTC):** {ts}",
            f"- **input path:** `{input_path}`",
            "",
            "## Failure",
            "",
            message,
            "",
            "Empty output CSV files with headers were generated so downstream steps can continue.",
        ]
    )
    (output_dir / "report.md").write_text(report, encoding="utf-8")



def main() -> None:
    try:
        table, meta = load_data(INPUT_PATH)
        core_rows, core_found, core_missing = build_core_rank_ranking(table)
        rating_rows, rating_found, rating_missing = build_rating_ranking(table)
        write_outputs(
            core_rows=core_rows,
            rating_rows=rating_rows,
            core_found=core_found,
            core_missing=core_missing,
            rating_found=rating_found,
            rating_missing=rating_missing,
            meta=meta,
            input_path=INPUT_PATH,
            output_dir=OUTPUT_DIR,
        )
    except Exception as exc:  # pragma: no cover - safety net for CI robustness
        write_failure_outputs(message=f"`{type(exc).__name__}`: {exc}")


if __name__ == "__main__":
    main()
