# Curriculum Progress and Learner Checkpoints

This document tracks **what the repository provides** and gives every learner a reusable assessment checklist. It is not a personal learning diary.

Updated: **2026-10-17**. Start with the [README](README.md) for prerequisites, Windows/macOS setup, and the complete curriculum.

## Table of contents

- [Status definitions](#status-definitions)
- [Curriculum delivery tracker](#curriculum-delivery-tracker)
- [Foundation checkpoints](#foundation-checkpoints)
- [Sample data and expected results](#sample-data-and-expected-results)
- [Current open exercise](#current-open-exercise)
- [Learning and architecture notes](#learning-and-architecture-notes)
- [Validation status](#validation-status)
- [Track your own progress](#track-your-own-progress)
- [Maintainer backlog](#maintainer-backlog)
- [Milestone history](#milestone-history)

## Status definitions

| Status | Meaning |
|---|---|
| Available | Referenced worked examples exist in the repository; not automatically runtime-validated |
| Partial | Some material exists, but the module's full objectives are not covered |
| Open exercise | Requirements exist; a completed implementation is not yet included |
| Planned | Curriculum intent only; no completed lesson is provided |
| Runtime-validated | A reproducible run and its environment/results have been recorded |

## Curriculum delivery tracker

Module numbers match the [README curriculum](README.md#curriculum-and-learning-path).

| Module | Delivery status | Current evidence or missing work |
|---|---|---|
| 00 — Orientation | Partial | Browser import guidance and setup cell exist; full platform orientation pending |
| 01 — PySpark foundations | Partial | Counts, filtering, grouping, renaming available; schemas and execution internals need fuller lessons |
| 02 — Joins and time | Partial | Left joins, scoped fill, timestamp/date parsing, calendar, cross join, and two-key join available; final daily report is open |
| 03 — Contracts and ingestion | Planned | Formal event contract, file fixture, malformed-record exercise |
| 04 — Delta and layered design | Planned | Persistent tables and layer-boundary exercises |
| 05 — Incremental reliability | Planned | Reruns, duplicates, late data, merge, safe refreshes and backfills |
| 06 — Engagement metrics | Planned | Historical dataset, windows, eligibility and business rules |
| 07 — Testing and orchestration | Planned | Automated checks, scheduling, retries and recovery |
| 08 — Governance and delivery | Planned | Catalog/access design, deployment and CI/CD |
| 09 — Performance and operations | Planned | Plans, measured experiments, monitoring and runbook |
| 10 — AI extension | Optional / planned | Grounded account summaries and evaluation |

## Foundation checkpoints

All current worked examples are in [00_getting_started.py](00_getting_started.py), in the sequence below. Check items in your own fork or learning notes only after reproducing and explaining them.

- [ ] **Environment:** import the notebook, run the setup cell, explain where compute runs.
- [ ] **Python counts:** count five events and three searches with Python.
- [ ] **DataFrames:** create the sample DataFrame and inspect its schema.
- [ ] **Filtering:** select searches without modifying the original DataFrame.
- [ ] **Aggregation:** count searches by account and events by account/type.
- [ ] **Schema clarity:** rename count columns to describe their meaning.
- [ ] **Post-aggregation filtering:** keep accounts with more than two events.
- [ ] **Account join:** retain all three accounts with a left join to unfiltered totals.
- [ ] **Missing counts:** explain null versus zero and the completeness assumption.
- [ ] **Scoped replacement:** fill only total_events, then select zero-activity accounts.
- [ ] **Timestamps:** parse event_ts while preserving the original string DataFrame.
- [ ] **Dates:** add event_date in the UTC session without removing event_ts.
- [ ] **Daily aggregation:** produce one row per observed account/date.
- [ ] **Reporting calendar:** build dates independently of incoming activity.
- [ ] **Cross join:** construct all six account/date combinations.
- [ ] **Two-key join:** attach daily counts using both account_id and event_date.
- [ ] **Final report:** complete the open exercise below.
- [ ] **Explanation:** distinguish transformations from actions and explain the grain at each stage.

The unchecked boxes are a blank learner template; they do not mean the corresponding worked examples are missing.

## Sample data and expected results

The original events fixture contains five actions:

| Account | Actions |
|---|---|
| a1 / Atlas | One search, one reveal, one export |
| a2 / Beacon | Two searches |
| a3 / Cedar | No events in the fixture |

Expected checks:
- Total events: **5**; searches: **3**.
- Search counts: a1 = **1**, a2 = **2**.
- Account totals after the left join: a1 = **3**, a2 = **2**, a3 = **null**.
- After justified account-level null replacement: a3 = **0**.
- The more-than-two-events filter retains only a1.

The separate timed fixture places three events on October 14 and two on October 15, 2026. It retains five event rows after timestamp parsing and date extraction.

The daily left join should produce the following rows, in any order:

| account_id | account_name | event_date | daily_events |
|---|---|---|---:|
| a1 | Atlas | 2026-10-14 | 2 |
| a1 | Atlas | 2026-10-15 | 1 |
| a2 | Beacon | 2026-10-14 | 1 |
| a2 | Beacon | 2026-10-15 | 1 |
| a3 | Cedar | 2026-10-14 | null |
| a3 | Cedar | 2026-10-15 | null |

These are expected results derived from the fixture, not captured execution logs.

## Current open exercise

**Goal:** finish the daily account report.

**Prerequisites:** complete the existing notebook through `daily_acc_activity`.

**Assumptions:** the synthetic data is complete for both reporting dates, and all three accounts are eligible on both dates.

Starting from `daily_acc_activity`:
1. Replace nulls only in `daily_events` with zero.
2. Sort by `account_id`, then `event_date`.
3. Assign the result to `daily_account_report` and display it.

Hints: use `fillna` and `orderBy`. The solution is intentionally not included yet.

Acceptance criteria:
- Exactly **six rows**, unique by account_id and event_date.
- Cedar has **zero** events on both dates.
- daily_events sums to **five**.
- Rows are explicitly sorted by account and date.
- The original daily_acc_activity still represents the unfilled result.

Reflection: why would applying the same null replacement to an incomplete reporting period be misleading?

## Learning and architecture notes

- **Grain:** event-level input, account-level totals, and account-date totals are different datasets with different join keys.
- **Missing data:** a null from a left join means no matching row; it alone does not prove zero real-world activity.
- **Failure behavior:** a truncate-first refresh can leave an empty target if it fails before insertion. Reporting must not present that failure as universal inactivity.
- **Join cardinality:** duplicate keys can repeat aggregate values. The supplied accounts fixture is unique; duplicate-account examples are hypothetical, not defects discovered in it.
- **Calendar design:** a date with no received events must still be representable in the reporting calendar.
- **Eligibility:** pairing all accounts with all dates is appropriate only under the exercise assumption. Account activation/deactivation windows are a later extension.
- **Time:** application event time, Kafka record timestamp, and ingestion time are distinct. The fixture models event time without implementing Kafka.
- **Business meaning:** lower search activity may reflect efficiency; sustained engagement rules require stakeholder definitions and trustworthy inputs.
- **Performance:** a Python list on one process is not a scalable ingestion design. Distributed execution and optimization must be studied with actual workloads.

## Validation status

| Area | Current evidence |
|---|---|
| Notebook source | Reconstructed from reviewed exercises; not an export of the live workspace |
| Python syntax | Checked for the current introductory notebook |
| Databricks runtime | No independently recorded clean-session run |
| Windows/macOS browser path | Same remote notebook workflow documented; no OS-specific runtime code required |
| Local Windows/macOS execution | Not packaged or validated |
| Automated tests | Not yet provided |
| Production infrastructure | Not implemented |

For a future runtime validation, record the date, workspace/compute type without secrets, runtime or environment version where available, commit SHA, commands/cells run, and observed outputs. Keep expected results separate from observed evidence.

## Track your own progress

Copy this template into your fork's notes or a learning journal:

```markdown
### Session YYYY-MM-DD
- Module/checkpoint:
- Repository commit:
- Environment:
- Exercise attempted:
- Result observed:
- What I can explain independently:
- Edge case or failure investigated:
- Remaining questions:
- Next task:
```

Finish a milestone when you can explain the concept, implement the behavior, verify the output, and diagnose a relevant failure. Do not equate reading a solution with demonstrating the skill.

## Maintainer backlog

Immediate:
- [ ] Complete and review the daily-report exercise.
- [ ] Record a clean-session Databricks run against a specific commit.
- [ ] Expand module introductions and add explicit learning objectives.
- [ ] Separate learner exercises from worked solutions as the course grows.

Next:
- [ ] Define an event contract, including event_id, account_id, user_id, event time, ingestion time, and allowed event types.
- [ ] Add file-based fixtures and malformed-record cases.
- [ ] Introduce Delta persistence and justify each processing layer.
- [ ] Add meaningful correctness checks before scheduling or deployment.
- [ ] Provide Windows and macOS instructions for future local tooling, with version requirements and validation evidence.
- [ ] Distinguish free-environment exercises from paid or architecture-only extensions.

## Milestone history

| Date | Material added |
|---|---|
| 2026-10-10 | Initial environment exercises, Python/PySpark counts, filters, grouping and renaming |
| 2026-10-16 | Left joins, null handling, zero-activity filtering and UTC timestamp parsing documented |
| 2026-10-17 | Dates, daily aggregation, reporting calendar and account-date joins added; guides restructured as a public curriculum |

History records material development, not completion by every learner.
