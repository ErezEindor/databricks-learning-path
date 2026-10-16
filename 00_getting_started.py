# Databricks notebook source
# MAGIC %md
# MAGIC # Account Pulse — Getting started
# MAGIC Reconstructed from Erez's submitted exercises on 2026-10-10 and 2026-10-16.
# MAGIC This is not a live-workspace export. Names and formatting are normalized.
# MAGIC Expected outputs are documented; this reconstruction has not been run in Databricks by the assistant.

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
# MAGIC ## Next task
# MAGIC Confirm expected counts: a1 = 3, a2 = 2, a3 = null.
# MAGIC Explain what a3's null means and what completeness checks are needed
# MAGIC before interpreting it as zero activity.
