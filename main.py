from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipeline import load_scoring_rules, run_pipeline
from salesforce import build_salesforce_export


BASE_DIR = Path(__file__).resolve().parent
INPUT_CSV_PATH = BASE_DIR / "messy_leads.csv"
SCORING_RULES_PATH = BASE_DIR / "scoring_rules.json"
OUTPUT_DIR = BASE_DIR / "outputs"


def main() -> None:
    raw_df = pd.read_csv(INPUT_CSV_PATH)
    scoring_rules = load_scoring_rules(SCORING_RULES_PATH)

    pipeline_result = run_pipeline(raw_df, scoring_rules)
    salesforce_export = build_salesforce_export(pipeline_result.clean_leads)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pipeline_result.clean_leads.to_csv(OUTPUT_DIR / "clean_leads.csv", index=False)
    pipeline_result.rejected_leads.to_csv(OUTPUT_DIR / "rejected_leads.csv", index=False)
    salesforce_export.to_csv(OUTPUT_DIR / "salesforce_export.csv", index=False)

    summary_report_path = OUTPUT_DIR / "summary_report.json"
    summary_report_path.write_text(
        json.dumps(pipeline_result.summary_report, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
