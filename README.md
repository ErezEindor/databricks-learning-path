# Databricks Learning Path — Account Pulse

A project-based learning journey by Erez Ein-Dor: build an account-engagement monitoring platform for a fictional B2B prospecting product using synthetic data.

## Goal

Turn searches, contact reveals, and exports into trustworthy account-level engagement metrics. Identify sustained changes and explain the evidence without treating lower activity as proof of churn.

Learning priorities:
- Reliable data pipelines with PySpark and Delta.
- Platform architecture and governance.
- Testing, deployment, monitoring, and recovery.
- Trustworthy data foundations for a later AI use case.

## Starting point and constraints

- Data engineering and SQL experience, plus experience designing a migration architecture.
- Learning hands-on Databricks and PySpark fundamentals.
- Comfortable with Git branches and pull requests.
- Personal Windows laptop; browser-based Databricks notebook setup completed.
- 2–3 learning hours per week; maximum budget of $20/month, with free options preferred.
- Planned learning environment: Databricks Free Edition. Exact account edition has not been independently verified.

## Current status

Reviewed exercises now cover Python counts, Spark DataFrame creation, filtering, aggregation, renaming, filtering aggregated results, left joins, scoped null replacement, zero-activity filtering, and UTC timestamp parsing.

Latest checkpoint: **2026-10-16**. Submitted code has been reviewed; displayed outputs have not been independently observed. We also documented the danger of interpreting a failed truncate-first refresh as zero activity.

**Next:** add an `event_date` column to `parsed_events_df` while preserving `event_ts`. Date extraction is assigned but not yet submitted.

See [PROGRESS.md](PROGRESS.md) for evidence, expected results, and the next task.

## Live Databricks notebook

[Open the Account Pulse learning notebook in Databricks](https://dbc-d59ea919-4a47.cloud.databricks.com/editor/notebooks/273428319476598?o=1380376101535698)

Requires sign-in and permission to access the Databricks workspace and notebook. This link does not grant access or automatically synchronize the notebook with this repository. The live notebook's contents have not been independently inspected by the assistant.

## Repository contents

| File | Purpose |
|---|---|
| [00_getting_started.py](00_getting_started.py) | Databricks source notebook reconstructed from the exercises submitted in conversation |
| [PROGRESS.md](PROGRESS.md) | Learning checkpoint, reviewed work, decisions, and next exercise |

The notebook is a reconstruction, not an export of the live workspace. Variable names and formatting are normalized, including the requested `total_events` output name. It has not been executed by the assistant in Databricks.

## Roadmap

Estimates assume 2–3 hours per week and are adjustable; progression depends on demonstrated understanding.

| Stage | Deliverable and learning focus | Estimate | Status |
|---|---|---|---|
| 1 | Setup, Python checkpoint, source contract, and table grains | 2 weeks | In progress: setup and initial exercises done; formal contract pending |
| 2 | First end-to-end pipeline; PySpark and Delta foundations | 4–5 weeks | Foundational PySpark exercises started; pipeline pending |
| 3 | Incremental ingestion, duplicates, schema changes, late events, safe reruns, backfills | 4–5 weeks | Planned |
| 4 | Daily/weekly metrics, historical baselines, decline rules, quality checks | 3–4 weeks | Planned |
| 5 | Orchestration, testing, deployment, monitoring, governance, recovery | 4–5 weeks | Planned |
| 6 | Performance experiments and optional grounded AI account brief | 3–4 weeks | Planned |

Testing, documentation, and governance will be introduced throughout. Enterprise administration and networking may require architecture exercises if unavailable in the learning environment. Completing one project is a foundation for mastery, not evidence of mastery by itself.

## Working method

1. Explain the concept and connect it to SQL or architecture.
2. Assign one manageable exercise with acceptance criteria.
3. Erez implements; hints precede full solutions unless requested.
4. Review correctness, output schema, readability, and operational behavior.
5. Introduce relevant failures and check understanding.
6. Record demonstrated skills, unresolved gaps, and the next task.

Business thresholds will be explicit fictional stakeholder assumptions. Separate observed metrics, business interpretations, and data validity. Do not silently equate missing data with zero activity.

## Progress maintenance

After each learning session:
- Record the session date and reviewed code or observed output in `PROGRESS.md`.
- Distinguish submitted code, user-confirmed execution, and independently verified output.
- Update the notebook with completed exercises.
- Keep unfinished work as tasks, rather than adding its solution as completed work.
- Keep all datasets synthetic; do not commit credentials, personal customer data, or company material.
