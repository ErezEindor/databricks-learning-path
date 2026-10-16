# Account Pulse — Progress

Documentation checkpoint: **2026-10-16**  
Learning sessions represented: **2026-10-10 and 2026-10-16**, Israel local time.

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
| Scoped null replacement | Submitted `acc_activity.fillna(0, subset="total_events")`; correct for the complete synthetic fixture |
| Zero-activity filtering | Submitted filter on `total_events == 0`; renamed result to `zero_activity_account` |
| Timestamp parsing | Submitted `F.to_timestamp(F.col("event_ts"))` inside `withColumn`, plus schema/display calls |
| Notebook setup | Placed functions import and UTC session configuration before timestamp work |
| Join selection | Selected and implemented a left join using unfiltered account totals; code reviewed on 2026-10-16, displayed output not shared |

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

The original sample has no timestamps. A separate five-row `timed_events` fixture now adds UTC event timestamps on October 14–15. User identifiers and a complete source contract are still pending.

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

## Left join exercise

**Status: implementation submitted and reviewed on 2026-10-16. Null interpretation discussed; output has not been independently observed.**

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

## Session checkpoint — 2026-10-16

Submitted code for scoped null replacement, zero-activity filtering, and timestamp parsing was reviewed as correct. Runtime outputs were not pasted or independently inspected.

Expected results:
- Filled account counts: a1 = 3, a2 = 2, a3 = 0.
- Zero-activity filter: only a3/Cedar, with total_events = 0.
- parsed_events_df: five rows; event_ts changes from string to timestamp, with no null timestamps for the supplied fixture.
- timed_events_df retains its string column; the original events_df is unchanged.

Architecture discussion:
- A truncate-first full refresh that fails before insertion leaves an empty events target. A left join from preserved accounts then produces null counts; filling those nulls would falsely report zero activity.
- A failed refresh must not publish an empty or partial dataset as a valid reporting period.
- Interpret unmatched counts as zero only after establishing completeness for the relevant reporting period. A higher reporting layer still needs that evidence.
- Event time, Kafka record timestamp, and ingestion time are distinct. event_ts models application event time; Kafka ingestion has not been implemented.
- The duplicate-account example was hypothetical. The actual accounts fixture has one row each for a1, a2, and a3. No duplicate defect was found or introduced.
- Join multiplication was explained, but an independent duplicate-detection exercise has not been completed.

## Next task — date extraction

**Status: assigned, not yet submitted.**

Add `event_date` to `parsed_events_df` using `withColumn`, `F.to_date`, and `F.col`. Assign the result to `daily_events_df` and display it.

Acceptance criteria:
- Five rows remain.
- Preserve `event_ts` as timestamp.
- `event_date` has date type and no nulls for this fixture.
- UTC calendar dates: three events on 2026-10-14 and two on 2026-10-15.

After review, proceed to daily per-account aggregation. Do not mark date extraction complete or add its solution yet.

## Not yet completed

- Full source contract, late-arrival behavior, and reporting-window semantics (event-time basics introduced).
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
