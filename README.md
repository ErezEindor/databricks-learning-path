# Databricks Learning Path

**Learn Databricks by building a data product, one engineering skill at a time.**

This repository is an evolving, project-based curriculum for anyone who wants to move from basic Python and SQL to reliable data engineering on Databricks. The running project, **Account Pulse**, turns synthetic product events into daily account-engagement metrics and, eventually, an explainable engagement monitoring platform.

**Current release:** an introductory notebook covering PySpark transformations through daily account-date joins. The broader curriculum below is a roadmap, not a claim that all lessons are already available.

## Table of contents

- [Who this is for](#who-this-is-for)
- [What you will build](#what-you-will-build)
- [Prerequisites](#prerequisites)
- [Start here — Windows and macOS](#start-here--windows-and-macos)
- [Available materials](#available-materials)
- [Curriculum and learning path](#curriculum-and-learning-path)
- [How to study](#how-to-study)
- [Milestones and assessment](#milestones-and-assessment)
- [Environment and cost](#environment-and-cost)
- [Project design principles](#project-design-principles)
- [Official references](#official-references)
- [Contributing](#contributing)
- [Maintainer workspace](#maintainer-workspace)

## Who this is for

- Data engineers learning Databricks or moving from a warehouse-oriented stack.
- SQL practitioners learning PySpark and distributed processing.
- Learners who want to connect architecture choices to working code, failure recovery, and operational behavior.

No prior Databricks experience is required. This is not yet a complete beginner Python course or a certification-preparation course.

## What you will build

Account Pulse models a fictional B2B prospecting product where users search for prospects, reveal contact details, and export results.

The central question is:

> Which accounts show sustained declining engagement, what changed, and can we trust the supporting data?

The first deliverable is a daily account activity report, including accounts with no activity. Later milestones add file ingestion, Delta tables, incremental processing, quality checks, historical comparisons, deployment, and observability. An optional AI extension will summarize verified account metrics.

All data is synthetic. The current fixture has three accounts, five events, and two reporting dates. User-level activity, realistic history, and production-style ingestion are planned extensions.

## Prerequisites

| Skill or resource | Needed at the start |
|---|---|
| Python | Variables, lists, dictionaries, conditions, and basic function calls |
| SQL | SELECT, WHERE, GROUP BY, and basic joins |
| Databricks account | Your own workspace with notebook execution access |
| Computer | Windows or macOS with a current browser and internet access |
| Git | Optional for the first notebook; useful for saving exercises and later collaboration |

If Python or SQL is new to you, practice those basics first. The notebook includes a small Python counting exercise to check readiness.

## Start here — Windows and macOS

The introductory path runs **inside a Databricks browser notebook**. Spark compute runs remotely; no local Spark, Java, Docker, Python installation, or terminal is required for these exercises.

| Task | Windows | macOS |
|---|---|---|
| Access Databricks | Browser | Browser |
| Download the source notebook | Save the raw .py file | Save the raw .py file |
| Run exercises | Import into your Databricks workspace | Import into your Databricks workspace |
| Optional local repository work | Git in PowerShell or Git Bash | Git in Terminal |
| Local Spark execution | Not configured by this repository | Not configured by this repository |

1. Create or use a personal learning workspace. [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) is a starting option; review its limits before selecting an environment.
2. Download [00_getting_started.py](00_getting_started.py) using GitHub's raw-file download. Preserve the `.py` extension and the first line, `# Databricks notebook source`.
3. In Databricks, open **Workspace**, choose a destination folder, and use **Import** to upload the file as a notebook. See the [official import instructions](https://docs.databricks.com/aws/en/notebooks/notebook-export-import) if your interface differs.
4. Open the imported notebook and select available compute; Free Edition uses serverless compute.
5. Run the setup cell first, then work through the notebook in order. The setup imports PySpark functions and sets the session timezone to UTC.
6. Compare results with the [checkpoint criteria](PROGRESS.md#foundation-checkpoints). Predict each result before running a worked example.
7. Complete the final exercise without copying a solution.

Optional, after installing Git, the following commands work in Windows PowerShell, Git Bash, and macOS Terminal:

```sh
git clone https://github.com/ErezEindor/databricks-learning-path.git
cd databricks-learning-path
```

Cloning is not required for the browser path. Do not run the notebook with plain `python 00_getting_started.py`: it expects Databricks-provided `spark` and `display`. Importing a file does not establish automatic Git synchronization.

## Available materials

| Material | What is available now |
|---|---|
| [00_getting_started.py](00_getting_started.py) | Worked examples for Python counts, DataFrames, filtering, grouping, renaming, joins, null handling, timestamps, dates, and a daily reporting calendar |
| [01_commands_reference.py](01_commands_reference.py) | Self-contained annotated reference for commands introduced through account reconciliation: syntax, SQL comparisons, expected results, and common mistakes |
| [PROGRESS.md](PROGRESS.md) | Curriculum delivery status, expected results, reusable learner checklist, and next exercise |
| This README | Setup, prerequisites, curriculum, milestones, and reference links |

The source notebook was reconstructed from reviewed learning exercises. Its Python syntax was checked, but a clean-session Databricks execution has not been independently verified. Windows and macOS use the same browser workflow; separate local-runtime support has not been tested or packaged.

## Curriculum and learning path

Study the modules in order. Later modules build on earlier outputs. The delivery status refers to repository materials, not to any individual learner's progress.

| Module | Topics to cover | Practical outcome | Material status |
|---|---|---|---|
| 00 — Orientation | Workspace, compute versus storage, notebooks, Git basics, Python checkpoint | Import and run the learning notebook | Starter examples available; orientation guide partial |
| 01 — PySpark foundations | Schemas, column expressions, filters, grouping, renaming, immutability, transformations and actions | Correct account-level metrics | Worked examples available; execution internals need expansion |
| 02 — Joins and time | Grain, join keys, null versus zero, UTC timestamps, dates, independent calendars, cross joins | Daily report retaining zero-activity account-days | Examples through daily join available; final exercise pending |
| 03 — Contracts and ingestion | Event schema, identifiers, event/ingestion time, explicit types, JSON/CSV files, malformed records | Validated file-based input and rejected-record handling | Planned |
| 04 — Delta and layered design | Delta tables, transaction concepts, schema enforcement, history, raw/clean/reporting responsibilities | Persisted pipeline with justified layer boundaries | Planned |
| 05 — Incremental reliability | Watermarks, deduplication, late events, MERGE, idempotency, schema evolution, safe refreshes and backfills | Repeatable processing that handles failure and reruns | Planned |
| 06 — Engagement metrics | Windows, historical baselines, account eligibility, weekly activity, completeness, explainable business rules | Evidence-backed engagement signals | Planned |
| 07 — Testing and orchestration | Data assertions, unit/integration tests, parameters, job dependencies, retries, logging, recovery | Scheduled, testable workflow | Planned |
| 08 — Governance and delivery | Unity Catalog concepts, ownership, permissions, lineage, environments, code review, CI/CD | Documented access model and reproducible deployment | Planned |
| 09 — Performance and operations | Query plans, shuffles, join strategies, skew, file layout, measurement, freshness and cost monitoring | Measured improvements and an operational runbook | Planned |
| 10 — AI extension | Grounded summaries, evaluation examples, traceability, limitations and cost | Account brief based on validated metrics | Optional / planned |

Streaming and Kafka ingestion may become a later extension. The current event payloads are not connected to Kafka.

## How to study

Keep [01_commands_reference.py](01_commands_reference.py) alongside the exercises when you need a command explained. It can be imported as a separate Databricks notebook and has its own fixtures. Reference examples do not imply that every exercise has been completed. Its Python syntax has been checked; a Databricks runtime run is not yet independently verified.

For each topic:
1. Read the concept and predict the output.
2. Implement a small exercise yourself.
3. Inspect the schema, row grain, and result.
4. Explain why the transformation is correct.
5. Try an edge case or failure scenario.
6. Record what you demonstrated and what remains unclear.

The introductory notebook currently contains worked examples plus an unfinished exercise, rather than a separate exercise/solution package. Use the checklist in PROGRESS.md to attempt the completed examples independently.

Suggested pace: 2–3 hours per week, split between concepts, implementation, and review. An initial end-to-end project may take several months; advanced material is still being developed. Passing a milestone matters more than meeting a calendar estimate.

## Milestones and assessment

| Milestone | Evidence of completion |
|---|---|
| A — Daily activity report | Correct grain, six sample account-date rows, five total events, deliberate missing-data handling |
| B — Persistent ingestion | Defined contract, file inputs, Delta outputs, rejected records with reasons |
| C — Reliable processing | Reruns preserve correctness; late data and a failed refresh are handled explicitly |
| D — Trustworthy engagement signals | Historical metrics and business rules distinguish low activity from incomplete data |
| E — Operable platform | Tests, scheduled runs, deployment instructions, access model, and recovery procedure |
| F — Optional AI brief | Claims trace back to metrics and are checked against evaluation examples |

For every milestone, demonstrate that you can explain the design, implement it, verify the result, and diagnose a relevant failure. One project is a foundation for deeper expertise, not a guarantee of mastery.

## Environment and cost

Start small and use the free environment where suitable. Free Edition is quota-limited and serverless-only; some enterprise administration and compute capabilities are unavailable. Check the [current limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

Later modules must identify whether an exercise works in the free environment or needs a paid environment. Where a capability is unavailable, provide an architecture exercise or a clearly labeled alternative. Paid exercises are not required by the current notebook. A spending alert should not be treated as a guaranteed hard spending cap.

## Project design principles

- Define the grain and output schema before joining or aggregating.
- Distinguish measured facts, business interpretation, and data validity.
- Lower activity is not automatically lower value or churn.
- Treat missing counts as zero only when the relevant reporting data is complete.
- Define reporting dates independently of observed events.
- Do not publish failed or partial refreshes as valid reporting periods.
- Keep raw event time available when deriving dates and aggregates.
- Add layers and infrastructure to solve demonstrated needs.
- Measure performance before choosing an optimization.
- Use synthetic data and keep credentials outside source control.

## Official references

Use these alongside the exercises; they are references, not claims of material already delivered here.

- [Databricks Free Edition setup](https://docs.databricks.com/aws/en/getting-started/free-edition)
- [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Notebook import and export](https://docs.databricks.com/aws/en/notebooks/notebook-export-import)
- [Apache Spark documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake documentation](https://docs.delta.io/latest/index.html)
- [Databricks documentation](https://docs.databricks.com/)

## Contributing

Propose improvements through issues or pull requests. For new lessons, include objectives, prerequisites, a synthetic fixture, an exercise, expected results, and validation evidence. State any paid-feature requirements and document Windows/macOS differences when local commands are introduced.

Keep available materials separate from planned topics. Do not mark a lesson runtime-validated based only on a syntax check. Avoid committing credentials, real customer data, or notebook outputs containing sensitive information.

## Maintainer workspace

[Maintainer's live Databricks notebook](https://dbc-d59ea919-4a47.cloud.databricks.com/editor/notebooks/273428319476598?o=1380376101535698)

This is an optional maintainer reference, not a prerequisite. It requires existing workspace permission and is not a public execution environment. Learners should import the repository notebook into their own workspace.
