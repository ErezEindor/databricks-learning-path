# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark commands reference — Account Pulse
# MAGIC A companion reference for every learner, covering commands introduced so far.
# MAGIC Import this .py file as a Databricks notebook and run top to bottom.
# MAGIC It defines its own tiny fixtures: no other notebook needs to run first.
# MAGIC Windows and macOS use the same browser workflow. Plain local Python is not configured.
# MAGIC No files or tables are written. Running this changes the session timezone to UTC.
# MAGIC
# MAGIC ## Contents
# MAGIC 1. Python basics and assignment
# MAGIC 2. Importing functions and configuring time
# MAGIC 3. Creating and inspecting DataFrames
# MAGIC 4. Column expressions and filtering
# MAGIC 5. Counting, grouping, aggregating, and naming
# MAGIC 6. Left joins and missing counts
# MAGIC 7. Timestamps, dates, and withColumn
# MAGIC 8. Calendars and cross joins
# MAGIC 9. Multiple join keys, fillna, and sorting
# MAGIC 10. select, distinct, and uniqueness
# MAGIC 11. first and scalar reconciliation
# MAGIC 12. Assertions and failure behavior
# MAGIC 13. Full outer joins and account reconciliation
# MAGIC 14. Execution model and common mistakes
# MAGIC 15. Official references
# MAGIC
# MAGIC This is a worked reference, not a record of each learner's completion.
# MAGIC Expected results are stated below; Databricks runtime execution has not been independently verified.

# COMMAND ----------

# 1. Python basics: a list contains dictionaries; each dictionary is one event.
events = [
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

# = assigns a Python name; == compares values.
# Assignment does not create a permanent Databricks table.
python_event_count = len(events)  # len: number of elements in this Python list.
python_search_count = sum(1 for event in events if event["event_type"] == "search")
# The generator supplies one for each match; Python sum adds them.
# f-strings insert values inside braces; print writes text.
print(f"Events: {python_event_count}; searches: {python_search_count}")  # 5; 3

# COMMAND ----------

# 2. Import convention: in a project notebook, keep imports near the top.
# F is an alias for Spark's functions module, not a DataFrame.
from pyspark.sql import functions as F

# spark is the SparkSession supplied by Databricks.
# conf.set sets a session configuration; UTC makes date boundaries explicit.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# 3. createDataFrame creates a Spark DataFrame from our small local fixture.
# Types are inferred here; explicit schemas are a later lesson.
# Do not first build a huge Python list for large-scale ingestion.
events_df = spark.createDataFrame(events)

# printSchema prints column names/types/nullability, not data rows.
events_df.printSchema()
# show prints a small sample and triggers execution.
events_df.show()
# display is a Databricks notebook helper, not a standard PySpark method.
# It requests results for an interactive table.
display(events_df)
# Do not assign df = df.show(): show prints output rather than returning a DataFrame.

# COMMAND ----------

# 4. col returns a Column expression, not a Python list of values.
# These reference a column: F.col("event_type"), events_df["event_type"],
# and events_df.event_type. Brackets/col avoid attribute-name collisions.
# SQL: SELECT * FROM events WHERE event_type = 'search'
searches_df = events_df.filter(F.col("event_type") == "search")
# Equivalent SQL-expression string syntax:
searches_sql_style = events_df.filter("event_type = 'search'")
display(searches_df)  # e1, e3, e5
# filter returns a new DataFrame; events_df still represents all five events.

# COMMAND ----------

# 5. DataFrame.count() is an action returning a Python integer.
print(events_df.count())  # 5

# groupBy returns GroupedData, ready for aggregation.
# GroupedData.count() returns a DataFrame of counts (not a Python integer).
# SQL: SELECT account_id, COUNT(*) FROM searches GROUP BY account_id
account_search_counts = searches_df.groupBy("account_id").count()
display(account_search_counts)  # a1: 1, a2: 2

# Multiple grouping keys set the result's grain.
account_event_counts = (
    events_df
    .groupBy("account_id", "event_type")
    .count()
    .withColumnRenamed("count", "event_count")
)
# withColumnRenamed returns a DataFrame with an existing column renamed.
display(account_event_counts)

# agg builds aggregate expressions; alias names an expression's output column.
# F.count("*") counts rows; F.count("event_id") counts only non-null IDs.
source_counts_by_acc = (
    events_df.groupBy("account_id")
    .agg(F.count("*").alias("source_events"))
)
# SQL HAVING: apply filter after aggregation.
high_activity = source_counts_by_acc.filter("source_events > 2")
display(high_activity)  # a1: 3

# COMMAND ----------

# 6. Left join: preserve every left row and attach right-side matches.
accounts_df = spark.createDataFrame([
    {"account_id": "a1", "account_name": "Atlas"},
    {"account_id": "a2", "account_name": "Beacon"},
    {"account_id": "a3", "account_name": "Cedar"},
])
account_totals = (
    events_df.groupBy("account_id").count()
    .withColumnRenamed("count", "total_events")
)
acc_activity = accounts_df.join(account_totals, on="account_id", how="left")
display(acc_activity)  # a1: 3, a2: 2, a3: null

# fillna returns a new DataFrame. subset restricts the affected columns.
# This fixture is complete, so the unmatched count is interpreted as zero.
# Missing ingestion in a real pipeline must not be disguised as zero activity.
acc_activity_complete = acc_activity.fillna(0, subset="total_events")
# Equivalent mapping syntax: acc_activity.fillna({"total_events": 0})
zero_activity_account = acc_activity_complete.filter(
    acc_activity_complete.total_events == 0
)
display(zero_activity_account)  # a3 / Cedar / 0
# Duplicate join keys can multiply rows. A join does not enforce uniqueness.

# COMMAND ----------

# 7. withColumn adds a column, or replaces it in the returned DataFrame
# if that name already exists. It does not modify the original DataFrame.
# to_timestamp parses strings; the Z in our strings identifies UTC.
parsed_events_df = events_df.withColumn(
    "event_ts", F.to_timestamp(F.col("event_ts"))
)
# Invalid strings can raise errors or yield null depending on runtime/settings.
# Do not assume malformed values are handled safely by default.
# SQL analogy: CAST(event_ts AS TIMESTAMP)

# to_date extracts a date; our session uses UTC date boundaries.
daily_event_df = parsed_events_df.withColumn(
    "event_date", F.to_date(F.col("event_ts"))
)
display(daily_event_df)  # five rows; event_ts retained; event_date added.

# COMMAND ----------

# 8. Daily aggregation: grain = account_id + event_date.
count_daily_event_df = (
    daily_event_df.groupBy("account_id", "event_date").count()
    .withColumnRenamed("count", "daily_events")
)
# Expected observed groups: a1/Oct14:2, a1/Oct15:1, a2/Oct14:1, a2/Oct15:1.

# A comma makes ("2026-10-14",) a one-element Python tuple.
# The second argument supplies the column name for these tuple rows.
reporting_dates_df = (
    spark.createDataFrame([("2026-10-14",), ("2026-10-15",)], ["event_date"])
    .withColumn("event_date", F.to_date(F.col("event_date")))
)
# crossJoin makes every pair: 3 accounts x 2 dates = 6 rows.
# SQL: SELECT * FROM accounts CROSS JOIN reporting_dates
account_dates_df = accounts_df.crossJoin(reporting_dates_df)
# Define dates independently of events so a day without events still exists.
# Assume all three accounts are eligible for both dates.
# Cross joins grow multiplicatively: keep the intended scope bounded.
# UNION stacks datasets; it does not produce all account-date combinations.

# COMMAND ----------

# 9. Join on BOTH fields to match the daily aggregate grain.
daily_acc_activity = account_dates_df.join(
    count_daily_event_df, on=["account_id", "event_date"], how="left"
)
# orderBy returns a sorted DataFrame; ascending is the default.
# Without explicit sorting, do not rely on displayed row order.
daily_account_report = (
    daily_acc_activity.fillna(0, subset="daily_events")
    .orderBy("account_id", "event_date")
)
display(daily_account_report)
# Expected: six rows; Cedar has two zeros; daily_events sums to five.
# Parentheses allow a readable multi-line method chain; each method receives
# the preceding result. Only display requests this chain's rows here.

# COMMAND ----------

# 10. select chooses columns/expressions; distinct removes duplicate full rows
# of the selected result. It does not repair the original dataset.
# SQL: SELECT DISTINCT account_id, event_date FROM report
row_count = daily_account_report.count()
unique_pairs = (
    daily_account_report.select("account_id", "event_date").distinct().count()
)
print(f"Rows: {row_count}, unique account-date pairs: {unique_pairs}")  # 6, 6
# This does not validate non-null keys or prove all required pairs are present.

# COMMAND ----------

# 11. agg without groupBy aggregates the whole DataFrame.
# F.sum adds numeric values, unlike count which counts rows/non-null values.
# SQL: SELECT SUM(daily_events) AS total_reported_events FROM report
event_tot_check = daily_account_report.agg(
    F.sum("daily_events").alias("total_reported_events")
)
display(event_tot_check)  # a one-row DataFrame with value 5

# first is an action returning a Row (or None for an empty DataFrame).
# Here the aggregate has one row, so extracting it is appropriate.
report_event_count = event_tot_check.first()["total_reported_events"]
source_event_count = events_df.count()
# A general unordered DataFrame has no guaranteed meaningful first row.
# SUM of an empty/all-null input can be null; do not assume it is always zero.

# COMMAND ----------

# 12. assert is Python, not a Spark transformation.
# True: continue silently. False: raise AssertionError and stop this cell.
assert row_count == unique_pairs, "Duplicate account-date pairs found"
assert source_event_count == report_event_count, (
    f"Event total mismatch: source={source_event_count}, "
    f"report={report_event_count}"
)
print("Checks passed: unique pairs and matching overall event totals")

# Deliberate failure demo, disabled so Run All can finish.
# Uncomment only for the exercise, then comment it out again.
# example_row_count = 7
# example_pair_unq = 6
# assert example_row_count == example_pair_unq, "Duplicate account-date pairs found"
# print("This line will not run after the failed assertion")

# Python optimization (-O) can disable assert statements. For mandatory production
# validation use explicit checks/exceptions or the pipeline's validation framework.
# Matching overall totals does not prove each account/date received the right count.

# COMMAND ----------

# 13. Account reconciliation: sum events across days, not the number of days.
report_counts_by_acc = (
    daily_account_report.groupBy("account_id")
    .agg(F.sum("daily_events").alias("reported_events"))
)
# full preserves keys from either side, unlike left which preserves only the left.
# SQL: source FULL OUTER JOIN report USING (account_id)
account_reconciliation = source_counts_by_acc.join(
    report_counts_by_acc, on="account_id", how="full"
)
display(account_reconciliation)
# Expected: a1 (3,3), a2 (2,2), a3 (null,0).
# The current learner exercise is to handle the source-side missing count
# deliberately, then investigate mismatches. That solution is not added here.
# A filter using != alone will not select null comparisons; null handling matters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Execution model and common mistakes
# MAGIC - DataFrames describe structured computations; transformations return new DataFrames.
# MAGIC - filter, select, distinct, joins, withColumn, fillna, and orderBy are transformations.
# MAGIC - groupBy().count() and agg() build aggregate DataFrames lazily.
# MAGIC - DataFrame.count(), first(), show(), and Databricks display request execution.
# MAGIC - F.col, F.sum, F.count, alias, to_date, and to_timestamp build expressions.
# MAGIC - printSchema inspects structure rather than requesting all data rows.
# MAGIC - Reusing a DataFrame variable does not automatically cache its computed results.
# MAGIC - Repeated actions can repeat work. We use them here for tiny educational fixtures.
# MAGIC - A variable assignment is not a permanent table write.
# MAGIC - Spark groupBy().count() counts rows; F.count("column") skips null values.
# MAGIC - Grouped counts change grain; join keys must match that grain.
# MAGIC - Never blanket-fill missing metrics without checking completeness.
# MAGIC - These checks are useful building blocks, not a complete production pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Official references
# MAGIC Match documentation to your workspace's Spark version when behavior differs.
# MAGIC - [DataFrame methods](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
# MAGIC - [Column functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
# MAGIC - [count and nulls](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count.html)
# MAGIC - [fillna](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.fillna.html)
# MAGIC - [to_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp.html)
# MAGIC - [Python assert](https://docs.python.org/3/reference/simple_stmts.html#the-assert-statement)
# MAGIC - [Databricks notebooks](https://docs.databricks.com/aws/en/notebooks/)
