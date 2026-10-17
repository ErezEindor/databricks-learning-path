# Databricks notebook source
# MAGIC %md
# MAGIC # Account Pulse — Getting started
# MAGIC Reconstructed from Erez's submitted exercises on 2026-10-10, 2026-10-16, and 2026-10-17.
# MAGIC This is not a live-workspace export. Names and formatting are normalized.
# MAGIC Expected outputs are documented; this reconstruction has not been run in Databricks by the assistant.

# COMMAND ----------

from pyspark.sql import functions as F

# Explicit UTC session assumption for timestamp display and date extraction.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

project_name = "Account Pulse"
print(f"{project_name} is ready")

# COMMAND ----------

events = [
    {"event_id": "e1", "account_id": "a1", "event_type": "search"},
    {"event_id": "e2", "account_id": "a1", "event_type": "reveal"},
    {"event_id": "e3", "account_id": "a2", "event_type": "search"},
    {"event_id": "e4", "account_id": "a1", "event_type": "export"},
    {"event_id": "e5", "account_id": "a2", "event_type": "search"},
]

count_events = len(events)
search_count = sum(1 for event in events if event["event_type"] == "search")
print(f"Found {count_events} events, {search_count} of which were searches")

# COMMAND ----------

# spark and display are provided by the Databricks notebook environment.
events_df = spark.createDataFrame(events)
events_df.printSchema()
events_df.show()

# COMMAND ----------

searches_df = events_df.filter(events_df.event_type == "search")
print(f"Total events: {events_df.count()}")
print(f"Search events: {searches_df.count()}")
display(events_df)
display(searches_df)

# COMMAND ----------

# Grain: one row per account with at least one search event.
account_search_counts = searches_df.groupBy("account_id").count()
display(account_search_counts)

# COMMAND ----------

# Grain: one row per observed (account_id, event_type) pair.
count_event_type = events_df.groupBy("account_id", "event_type").count()
display(count_event_type)

# COMMAND ----------

account_event_counts = count_event_type.withColumnRenamed("count", "event_count")
display(account_event_counts)

# COMMAND ----------

# Review correction: use the required total_events output name.
events_higher_than_2 = (
    events_df
    .groupBy("account_id")
    .count()
    .withColumnRenamed("count", "total_events")
    .filter("total_events > 2")
)
display(events_higher_than_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Left join — submitted 2026-10-16
# MAGIC Code reviewed; displayed output has not been shared.
# MAGIC Accounts setup below is the supplied exercise fixture.

# COMMAND ----------

accounts = [
    {"account_id": "a1", "account_name": "Atlas"},
    {"account_id": "a2", "account_name": "Beacon"},
    {"account_id": "a3", "account_name": "Cedar"},
]
accounts_df = spark.createDataFrame(accounts)

# COMMAND ----------

total_acc = (
    events_df
    .groupBy("account_id")
    .count()
    .withColumnRenamed("count", "total_events")
)

acc_activity = accounts_df.join(total_acc, on="account_id", how="left")
display(acc_activity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null handling and zero activity
# MAGIC This synthetic fixture is assumed complete. In production, validate
# MAGIC reporting-period completeness before interpreting missing matches as zero.

# COMMAND ----------

acc_activity_complete = acc_activity.fillna(0, subset="total_events")
display(acc_activity_complete)

# COMMAND ----------

zero_activity_account = acc_activity_complete.filter(
    acc_activity_complete.total_events == 0
)
display(zero_activity_account)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Timestamp parsing — submitted 2026-10-16
# MAGIC event_ts models when the application action happened.
# MAGIC These records are synthetic payloads, not records ingested from Kafka.
# MAGIC Keep the original events dataset unchanged.

# COMMAND ----------

timed_events = [
    {"event_id": "e1", "account_id": "a1", "event_type": "search",
     "event_ts": "2026-10-14T09:00:00Z"},
    {"event_id": "e2", "account_id": "a1", "event_type": "reveal",
     "event_ts": "2026-10-14T09:05:00Z"},
    {"event_id": "e3", "account_id": "a2", "event_type": "search",
     "event_ts": "2026-10-14T10:00:00Z"},
    {"event_id": "e4", "account_id": "a1", "event_type": "export",
     "event_ts": "2026-10-15T11:00:00Z"},
    {"event_id": "e5", "account_id": "a2", "event_type": "search",
     "event_ts": "2026-10-15T12:00:00Z"},
]

timed_events_df = spark.createDataFrame(timed_events)
timed_events_df.printSchema()

# COMMAND ----------

parsed_events_df = timed_events_df.withColumn(
    "event_ts",
    F.to_timestamp(F.col("event_ts"))
)
parsed_events_df.printSchema()
display(parsed_events_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily activity — submitted 2026-10-17
# MAGIC Code reviewed; runtime output has not been independently observed.

# COMMAND ----------

daily_event_df = parsed_events_df.withColumn(
    "event_date", F.to_date(F.col("event_ts"))
)
display(daily_event_df)

# COMMAND ----------

# Grain: one row per account and UTC date with recorded activity.
count_daily_event_df = (
    daily_event_df
    .groupBy("account_id", "event_date")
    .count()
    .withColumnRenamed("count", "daily_events")
)
display(count_daily_event_df)

# COMMAND ----------

# Explicit calendar independent of received events.
# Exercise assumption: all three accounts are eligible on both reporting dates.
reporting_dates_df = (
    spark.createDataFrame(
        [("2026-10-14",), ("2026-10-15",)],
        ["event_date"]
    )
    .withColumn("event_date", F.to_date(F.col("event_date")))
)

account_dates_df = accounts_df.crossJoin(reporting_dates_df)
display(account_dates_df)

# COMMAND ----------

# Match the account-date grain on both keys; preserve all six combinations.
daily_acc_activity = account_dates_df.join(
    count_daily_event_df,
    on=["account_id", "event_date"],
    how="left"
)
display(daily_acc_activity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next task — finish the daily report (pending)
# MAGIC Assuming the synthetic reporting periods are complete:
# MAGIC 1. Replace nulls only in daily_events with zero.
# MAGIC 2. Sort by account_id, then event_date.
# MAGIC 3. Assign to daily_account_report and display it.
# MAGIC Expected: six rows, two zero counts for Cedar, and counts summing to five.
# MAGIC The solution has not yet been submitted and is intentionally omitted.
