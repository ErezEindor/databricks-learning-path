# Account Pulse — Progress

Documentation checkpoint: **2026-10-17**  
Learning sessions represented: **2026-10-10, 2026-10-16, and 2026-10-17**, Israel local time.

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
| Date extraction | Submitted `F.to_date(F.col("event_ts"))` into new event_date column; preserves timestamp |
| Daily aggregation | Submitted grouping on account_id and event_date, with daily_events count |
| Reporting calendar | Created two explicit dates independently of event data |
| Account-date combinations | Submitted crossJoin; expected 3 accounts × 2 dates = 6 rows |
| Daily left join | Submitted join on both account_id and event_date, retaining missing activity as null |
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

## Session checkpoint — 2026-10-17

Date extraction, daily aggregation, the independent reporting calendar, account-date cross join, and two-key left join were submitted and reviewed. Code is preserved in the reconstructed notebook, with formatting normalized and the user's variable names retained. Runtime output has not been shared or independently verified.

Expected progression:
- `daily_event_df`: five event rows, preserving event_ts and adding event_date.
- `count_daily_event_df`: four observed account-date groups.
- `account_dates_df`: six combinations (three accounts × two reporting dates).
- `daily_acc_activity`: six rows after joining on both account_id and event_date.

Expected daily join output, in any order:

| account_id | account_name | event_date | daily_events |
|---|---|---|---|
| a1 | Atlas | 2026-10-14 | 2 |
| a1 | Atlas | 2026-10-15 | 1 |
| a2 | Beacon | 2026-10-14 | 1 |
| a2 | Beacon | 2026-10-15 | 1 |
| a3 | Cedar | 2026-10-14 | null |
| a3 | Cedar | 2026-10-15 | null |

Decisions and concepts:
- UNION combines rows; CROSS JOIN generates every pair. The distinction was explained, then the cross join was implemented correctly.
- Reporting dates are defined independently of received events, so a completely missing day can still appear.
- All three accounts are assumed eligible for both dates in this fixture; real account eligibility is not yet modeled.
- The daily count grain is one row per account and date. Both fields are needed for the join.
- Null daily counts are intentionally preserved at this checkpoint.

## Next task — finish the daily report

**Status: assigned, not yet submitted.**

Starting with `daily_acc_activity`, under the complete-synthetic-data assumption:
1. Replace nulls only in `daily_events` with zero.
2. Sort by `account_id`, then `event_date`.
3. Assign the result to `daily_account_report` and display it.

Acceptance criteria:
- Exactly six rows, one per account-date combination.
- Cedar has zero events on both dates.
- Daily counts sum to five.
- Account and date sorting is explicit.
- Original `daily_acc_activity` retains nulls.

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
