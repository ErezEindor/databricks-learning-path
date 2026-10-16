# Account Pulse — Progress

Documentation checkpoint: **2026-10-16**  
Learning sessions represented: **2026-10-10**, Israel local time.

This record is based on the learning conversation and submitted code. Expected outputs below are derived from the five-event sample; they are not independently captured Databricks execution results.

## Completed or demonstrated

| Topic | Evidence and status |
|---|---|
| Workspace setup | User shared workspace screenshots and confirmed creating the project folder/notebook |
| Python execution | User confirmed completing the initial print exercise |
| Python collection processing | Submitted `len(events)` and a generator expression counting search events; logic correct |
| Spark basics | Submitted DataFrame counts, filter, and display calls |
| Immutability | Correctly predicted that filtering into a new DataFrame leaves the original with five rows |
| Grouping by account | Submitted `searches_df.groupBy("account_id").count()` |
| Grouping by account and event type | Submitted `events_df.groupBy("account_id", "event_type").count()` |
| Column renaming | Submitted `withColumnRenamed("count", "event_count")` |
| Filtering aggregated results | Submitted account aggregation followed by `filter("event_count > 2")`; logic correct, but required output name was `total_events` |
| Join selection | Correctly selected a left join to retain accounts without events; implementation not yet submitted |

Lazy evaluation, transformations versus actions, and aggregation grain were introduced. Independent mastery of execution planning has not yet been demonstrated.

## Sample and expected outputs

Five synthetic events:
- a1: one search, one reveal, one export.
- a2: two searches.

Expected results:
- Total events: 5.
- Search events: 3.
- Search counts: a1 = 1, a2 = 2.
- Counts by account and event type: a1/search = 1, a1/reveal = 1, a1/export = 1, a2/search = 2.
- Accounts with more than two total events: a1 = 3.

The current sample has no users or timestamps and is not yet a complete source contract.

## Decisions and lessons

- Lower search volume can reflect greater efficiency; assess several signals before interpreting it.
- Business stakeholders own risk definitions; engineering clarifies, challenges, implements, and validates them.
- Sustained decline and an isolated unusual period must be distinguishable.
- Preserve zero-activity accounts in reporting.
- A Python list stores objects in one process; large datasets should be read directly by Spark rather than first collected into a huge Python list.
- Traditional indexes were discussed conceptually; no indexing or storage-layout optimization has been implemented.
- Optimize using actual queries and measured execution behavior.
- Output column names are part of the consumer-facing data contract.
- Spark transformations return new DataFrames; grouping changes the result grain.

## Next exercise — left join

**Status: pending implementation.**

Create this synthetic accounts dataset:

| account_id | account_name |
|---|---|
| a1 | Atlas |
| a2 | Beacon |
| a3 | Cedar |

Tasks:
1. Create `accounts_df`.
2. Aggregate all events in `events_df` by account into `account_totals`.
3. Name the count column `total_events`.
4. Left join accounts to those totals using `account_id`.
5. Display the result; leave nulls unchanged for discussion.

Acceptance criteria:

| account_id | account_name | total_events |
|---|---|---|
| a1 | Atlas | 3 |
| a2 | Beacon | 2 |
| a3 | Cedar | null |

- Exactly three rows for this unique-account sample, one per account.
- Do not reuse the totals filtered to more than two events; that would lose a2's count.
- Explain why a3 has null rather than a measured zero.
- Discuss the completeness assumptions needed before converting null to zero.

The follow-up is to examine missing matches and data completeness, then handle nulls deliberately.

## Not yet completed

- Full source contract and event-time semantics.
- Formal Python/testing proficiency assessment beyond the submitted exercises.
- Delta table persistence, file ingestion, or any production pipeline.
- Scheduled jobs, automated tests, CI/CD, access controls, monitoring, or recovery.
- Performance measurements, large-scale processing, or AI functionality.
- Synchronization of the live Databricks notebook with this repository.

## Session checkpoint template

For future updates, record:
- Date:
- Task and acceptance criteria:
- Code reviewed / output observed:
- Skills demonstrated:
- Decisions and assumptions:
- Gaps or blockers:
- Next task:
