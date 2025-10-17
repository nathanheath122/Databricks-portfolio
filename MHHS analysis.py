# Databricks notebook source
# MAGIC %md
# MAGIC #HH Analysis
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Returning a table giving the hh consumption for hh mpans returning the mpans consumption for the first 7 days of the months January, April, July and October to get a yearly overview of that mpans consumption

# COMMAND ----------

# DBTITLE 1,HH Consumption for HH
# from pyspark.sql.functions import col, current_date, date_sub, row_number, when, coalesce, to_date, lit
# from pyspark.sql.window import Window

# # ───────────────────────────────────────────────
# # Load tables
# # ───────────────────────────────────────────────
# workinghhunique = spark.table("refined.industry.smart_meter_half_hourly_readings")
# junifercustomer = spark.table("refined.customer.customer")
# agreements = spark.table("refined.customer.agreements")
# accounts = spark.table("refined.customer.account")
# afmsdata = spark.table("source_systems.afms_elec_evo_dbo.mpan")

# # ───────────────────────────────────────────────
# # Filter HH readings for electricity + import
# # ───────────────────────────────────────────────
# elec_import = workinghhunique.filter(
#     (col("is_elec") == True) &
#     (col("is_import") == True)
# )

# # ───────────────────────────────────────────────
# # Find MPXNs with a reading in the last 7 days
# # ───────────────────────────────────────────────
# recent_mpxns = (
#     elec_import
#     .filter(col("timestamp") >= date_sub(current_date(), 7))
#     .select("ImportMPXN")
#     .distinct()
# )

# # ───────────────────────────────────────────────
# # Keep all readings for these MPXNs in the last 3 months
# # ───────────────────────────────────────────────
# from pyspark.sql.functions import month, dayofmonth, year, add_months

# # Define last 12 months window
# one_year_ago = add_months(current_date(), -12)

# all_recent_mpxn_reads = (
#     elec_import
#     .join(recent_mpxns, ["ImportMPXN"], "inner")
#     .filter(col("timestamp") >= one_year_ago)  # last 12 months only
#     .filter(
#         (month("timestamp").isin(1, 4, 7, 10)) &   # Jan, Apr, Jul, Oct
#         (dayofmonth("timestamp") <= 7)             # first 7 days
#     )
# )


# # ───────────────────────────────────────────────
# # Dedup AFMS to exactly one row per MPXN
# # ───────────────────────────────────────────────
# is_active = when(col("J0117").isNull(), lit(1)).otherwise(lit(0))
# end_dt_key = coalesce(col("J0117"), to_date(lit("9999-12-31")))
# ssd_key = coalesce(col("J0049"), to_date(lit("1900-01-01")))

# afms_win = Window.partitionBy("J0003").orderBy(
#     is_active.desc(),
#     end_dt_key.desc(),
#     ssd_key.desc()
# )

# afms_one_per_mpxn = (
#     afmsdata
#     .withColumn("rn_afms", row_number().over(afms_win))
#     .filter(col("rn_afms") == 1)
#     .drop("rn_afms")
# )

# # ───────────────────────────────────────────────
# # Dedup agreements (earliest non-cancelled per meterpoint)
# # ───────────────────────────────────────────────
# agr_win = Window.partitionBy("meterpoint_fk").orderBy(col("from_date").asc())
# dedup_agreements = (
#     agreements
#     .filter(col("flags.cancelled_flag") == False)
#     .withColumn("rn_agr", row_number().over(agr_win))
#     .filter(col("rn_agr") == 1)
#     .drop("rn_agr")
# )

# # ───────────────────────────────────────────────
# # Join reads → AFMS → Agreement → Account → Customer
# # ───────────────────────────────────────────────
# joined = (
#     all_recent_mpxn_reads.alias("hh")
#     .join(afms_one_per_mpxn.alias("afms"), col("hh.ImportMPXN") == col("afms.J0003"), "left")
#     .join(dedup_agreements.alias("agr"), col("afms.PK") == col("agr.meterpoint_fk"), "left")
#     .join(accounts.alias("acct"), col("agr.account_fk") == col("acct.account_pk"), "left")
#     .join(junifercustomer.alias("cust"), col("acct.customer_fk") == col("cust.customer_pk"), "left")
# )

# result = joined.select(
#     # HH readings
#     col("hh.ImportMPXN"),
#     col("hh.Consumption"),
#     col("hh.timestamp"),
    
#     # AFMS columns
#     col("afms.DISTRIBUTOR"),
#     col("afms.J0066").alias("GSP"),
#     col("afms.J0220"),
#     col("afms.J1043").alias("City"),
#     col("afms.J0263").alias("postcode"),
    
#     # Customer columns
#     col("cust.customer_name"),
#     col("cust.customer_type")
# )

# display(result)
# print("Row count:", result.count())
# result.write.mode("overwrite").saveAsTable("analysts.idm_and_settlement.MHHS_HH_Analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC # NHH Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivoting the nhh elec volumes to allow for in depth analysis of daily trends in consumption

# COMMAND ----------

# DBTITLE 1,nhh elec vol pivot
# from pyspark.sql import functions as F
# from datetime import date, timedelta

# # Calculate the cutoff date for the last 6 months
# months_ago = date.today().replace(day=1) - timedelta(days=14)

# # Load NHH electricity volumes table
# df = spark.table("source_systems.industry_nhh_elec_volumes.nhh_elec_volumes")

# # Filter early by consumption_month
# df_filtered_early = df.filter(F.col("consumption_month") >= F.lit(months_ago))

# # Columns you want to unpivot (exclude period_49 and period_50)
# period_cols = [f"period_{i}_volume" for i in range(1, 49)]

# # Columns you actually want to keep
# selected_cols = [
#     "mpan",
#     "settlement_date",
#     "tpr",
#     "annualised_advance",
#     "estimated_annual_consumption",
#     "profile_class",
#     "ssc",
#     "measurement_class",
#     "gsp",
#     "volume",
#     "source",
#     "consumption_month"
# ]

# # Unpivot period columns using stack
# df_long = df_filtered_early.select(
#     *selected_cols,
#     F.expr(
#         "stack({0}, {1}) as (period, hh_volume)".format(
#             len(period_cols),
#             ", ".join([f"{i}, {col}" for i, col in enumerate(period_cols, start=1)])
#         )
#     )
# )

# # Load GSP table to add customer_name
# gsp = spark.table("analysts.idm_and_settlement.gsp").select("mpan", "customer_name")

# # Join df_long with gsp
# df_enriched = df_long.join(gsp, on="mpan", how="left")

# # Save as table
# df_enriched.write.mode("overwrite").saveAsTable("analysts.idm_and_settlement.nhh_elec_volumes_pivoted")

# display(df_enriched)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping NHH Commercial into groups for migration based on R2 Performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a table for nhh commercial settlement performance but only for the mpans that are not with us.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE analysts.idm_and_settlement.nhh_commercial_settlement_performance_with_us_r2 AS
# MAGIC -- settlement dates
# MAGIC with settlement_dates as (
# MAGIC   select run as sd_run,
# MAGIC          max(date) as settlement_date
# MAGIC   from analysts.idm_and_settlement.settlement_runs
# MAGIC   where `DA Run Date` = (
# MAGIC       select min(`DA Run Date`)
# MAGIC       from analysts.idm_and_settlement.settlement_runs
# MAGIC       where `DA Run Date` >= current_date() + 1
# MAGIC   )
# MAGIC   group by run
# MAGIC ),
# MAGIC -- commercial mpans (base filter, still active today)
# MAGIC mpans as (
# MAGIC   select *
# MAGIC   from analysts.idm_and_settlement.gsp
# MAGIC   where affecting_settlement = true
# MAGIC     and customer_type = 'Commercial'
# MAGIC     and export_indicator is false
# MAGIC     and supply_end_date is null
# MAGIC ),
# MAGIC -- not settled at R2
# MAGIC R2 as (
# MAGIC   select *
# MAGIC   from mpans
# MAGIC   where run in ('RF', 'R3', 'R2')
# MAGIC ),
# MAGIC -- settled at R2
# MAGIC R2_s as (
# MAGIC   select *
# MAGIC   from mpans
# MAGIC   left join settlement_dates on settlement_dates.sd_run = 'R2'
# MAGIC   where run not in ('RF', 'R3', 'R2') 
# MAGIC     and supply_start_date <= settlement_date
# MAGIC     and (supply_end_date >= settlement_date or supply_end_date is null)
# MAGIC ),
# MAGIC -- aggregated R2 error
# MAGIC agg_R2 as (
# MAGIC   select 
# MAGIC     coalesce(customer_name, 'unknown') as customer_name,
# MAGIC     coalesce(meter_type, 'unknown') as meter_type,
# MAGIC     sum(eac) as volume_error, 
# MAGIC     count(eac) as count_error 
# MAGIC   from R2 
# MAGIC   group by customer_name, meter_type
# MAGIC ),
# MAGIC -- aggregated R2 settled
# MAGIC agg_R2_s as (
# MAGIC   select 
# MAGIC     coalesce(customer_name, 'unknown') as customer_name,
# MAGIC     coalesce(meter_type, 'unknown') as meter_type,
# MAGIC     sum(eac) as volume_settled, 
# MAGIC     count(eac) as count_settled 
# MAGIC   from R2_s 
# MAGIC   group by customer_name, meter_type
# MAGIC ),
# MAGIC -- final R2 results
# MAGIC r2_results as (
# MAGIC   select
# MAGIC     coalesce(agg_r2_s.customer_name, agg_r2.customer_name) as customer_name,
# MAGIC     coalesce(agg_r2_s.meter_type, agg_r2.meter_type) as meter_type,
# MAGIC     round(coalesce(agg_r2_s.volume_settled, 0), 2) as r2_volume_settled,
# MAGIC     coalesce(agg_r2_s.count_settled, 0) as r2_count_settled,
# MAGIC     round(coalesce(agg_r2.volume_error, 0), 2) as r2_volume_error,
# MAGIC     coalesce(agg_r2.count_error, 0) as r2_count_error,
# MAGIC     round((coalesce(agg_r2_s.volume_settled, 0) + coalesce(agg_r2.volume_error, 0)), 2) as r2_total_volume,
# MAGIC     coalesce(agg_r2.count_error, 0) + coalesce(agg_r2_s.count_settled, 0) as r2_total_count,
# MAGIC     round(
# MAGIC       coalesce(
# MAGIC         coalesce(agg_r2_s.volume_settled, 0) / nullif((coalesce(agg_r2_s.volume_settled, 0) + coalesce(agg_r2.volume_error, 0)), 0), 0
# MAGIC       ), 4
# MAGIC     ) as r2_performance
# MAGIC   from agg_r2_s
# MAGIC   full outer join agg_r2
# MAGIC     on agg_r2.customer_name = agg_r2_s.customer_name
# MAGIC     and agg_r2.meter_type = agg_r2_s.meter_type
# MAGIC )
# MAGIC select *
# MAGIC from r2_results
# MAGIC order by r2_volume_error desc;
# MAGIC select * from analysts.idm_and_settlement.nhh_commercial_settlement_performance_with_us_r2;

# COMMAND ----------

# DBTITLE 1,nhh comm group r2
# MAGIC %sql
# MAGIC -- -- 1️⃣ Create the table with MHHS Groups
# MAGIC -- CREATE OR REPLACE TABLE analysts.idm_and_settlement.nhh_commercial_MHHS_Groups AS
# MAGIC -- WITH base AS (
# MAGIC --     -- Pull all NHH commercial settlement performance data
# MAGIC --     SELECT 
# MAGIC --         customer_name,
# MAGIC --         meter_type,
# MAGIC --         r2_volume_settled,
# MAGIC --         r2_count_settled,
# MAGIC --         r2_volume_error,
# MAGIC --         r2_count_error,
# MAGIC --         r2_total_volume,
# MAGIC --         r2_total_count,
# MAGIC --         r2_performance
# MAGIC --     FROM analysts.idm_and_settlement.nhh_commercial_settlement_performance_with_us_r2
# MAGIC -- ),
# MAGIC -- -- total MPAN for reference
# MAGIC -- total_mpan AS (
# MAGIC --     SELECT SUM(r2_total_count) AS total_mpan
# MAGIC --     FROM base
# MAGIC -- ),
# MAGIC -- -- classify into performance buckets
# MAGIC -- group1_candidates AS (
# MAGIC --     SELECT *,
# MAGIC --         CASE 
# MAGIC --             WHEN r2_performance = 1 THEN 'best'
# MAGIC --             WHEN r2_performance > 0.7 THEN 'good'
# MAGIC --             WHEN r2_performance > 0.3 THEN 'alright'
# MAGIC --             ELSE 'poor'
# MAGIC --         END AS perf_bucket
# MAGIC --     FROM base
# MAGIC -- ),
# MAGIC -- -- order customers inside each bucket by size
# MAGIC -- group1_ordered AS (
# MAGIC --     SELECT *,
# MAGIC --         ROW_NUMBER() OVER (PARTITION BY perf_bucket ORDER BY customer_name DESC) AS rn
# MAGIC --     FROM group1_candidates
# MAGIC -- ),
# MAGIC -- -- cumulative MPAN per bucket
# MAGIC -- group1_cumulative AS (
# MAGIC --     SELECT g.*,
# MAGIC --         SUM(r2_total_count) OVER (PARTITION BY perf_bucket ORDER BY rn) AS cum_count
# MAGIC --     FROM group1_ordered g
# MAGIC -- ),
# MAGIC -- -- pick enough customers from each bucket to hit target MPAN %
# MAGIC -- group1 AS (
# MAGIC --     SELECT g.*, 1 AS grp
# MAGIC --     FROM group1_cumulative g
# MAGIC --     CROSS JOIN total_mpan t
# MAGIC --     WHERE 
# MAGIC --         (perf_bucket = 'best' AND cum_count <= 0.01 * t.total_mpan) OR
# MAGIC --         (perf_bucket = 'good' AND cum_count <= 0.05 * t.total_mpan) OR
# MAGIC --         (perf_bucket = 'alright' AND cum_count <= 0.03 * t.total_mpan) OR
# MAGIC --         (perf_bucket = 'poor' AND cum_count <= 0.01 * t.total_mpan)
# MAGIC -- ),
# MAGIC -- -- Remaining customers after Group 1
# MAGIC -- remaining AS (
# MAGIC --     SELECT b.*
# MAGIC --     FROM base b
# MAGIC --     LEFT JOIN group1 g
# MAGIC --         ON b.customer_name = g.customer_name
# MAGIC --         AND b.meter_type = g.meter_type
# MAGIC --     WHERE g.customer_name IS NULL
# MAGIC -- ),
# MAGIC -- -- Split remaining customers into 9 performance bands (for Groups 2–10)
# MAGIC -- banded AS (
# MAGIC --     SELECT r.*, 
# MAGIC --         NTILE(9) OVER (ORDER BY r2_performance DESC) AS performance_band
# MAGIC --     FROM remaining r
# MAGIC -- ),
# MAGIC -- -- Compute ordering key for snake logic
# MAGIC -- band_with_order_key AS (
# MAGIC --     SELECT *,
# MAGIC --         CASE 
# MAGIC --             WHEN performance_band % 2 = 1 THEN -r2_total_count  -- odd bands: largest first
# MAGIC --             ELSE r2_total_count                                  -- even bands: smallest first
# MAGIC --         END AS order_key
# MAGIC --     FROM banded
# MAGIC -- ),
# MAGIC -- -- Assign row numbers inside each band
# MAGIC -- band_positioned AS (
# MAGIC --     SELECT *,
# MAGIC --         ROW_NUMBER() OVER (PARTITION BY performance_band ORDER BY order_key) AS band_position
# MAGIC --     FROM band_with_order_key
# MAGIC -- ),
# MAGIC -- -- Assign Groups 2–10 using snake ordering
# MAGIC -- assigned_groups AS (
# MAGIC --     SELECT 
# MAGIC --         customer_name,
# MAGIC --         meter_type,
# MAGIC --         r2_volume_settled,
# MAGIC --         r2_count_settled,
# MAGIC --         r2_volume_error,
# MAGIC --         r2_count_error,
# MAGIC --         r2_total_volume,
# MAGIC --         r2_total_count,
# MAGIC --         r2_performance,
# MAGIC --         ((band_position - 1) % 20) + 2 AS grp
# MAGIC --     FROM band_positioned
# MAGIC -- ),
# MAGIC -- -- Combine Group 1 and Groups 2–10
# MAGIC -- final AS (
# MAGIC --     SELECT 
# MAGIC --         customer_name,
# MAGIC --         meter_type,
# MAGIC --         r2_volume_settled,
# MAGIC --         r2_count_settled,
# MAGIC --         r2_volume_error,
# MAGIC --         r2_count_error,
# MAGIC --         r2_total_volume,
# MAGIC --         r2_total_count,
# MAGIC --         r2_performance,
# MAGIC --         grp
# MAGIC --     FROM group1
# MAGIC
# MAGIC --     UNION ALL
# MAGIC
# MAGIC --     SELECT 
# MAGIC --         customer_name,
# MAGIC --         meter_type,
# MAGIC --         r2_volume_settled,
# MAGIC --         r2_count_settled,
# MAGIC --         r2_volume_error,
# MAGIC --         r2_count_error,
# MAGIC --         r2_total_volume,
# MAGIC --         r2_total_count,
# MAGIC --         r2_performance,
# MAGIC --         grp
# MAGIC --     FROM assigned_groups
# MAGIC -- )
# MAGIC -- -- Materialize the table
# MAGIC -- SELECT 
# MAGIC --     customer_name,
# MAGIC --     meter_type,
# MAGIC --     r2_volume_settled,
# MAGIC --     r2_count_settled,
# MAGIC --     r2_volume_error,
# MAGIC --     r2_count_error,
# MAGIC --     r2_total_volume,
# MAGIC --     r2_total_count,
# MAGIC --     r2_performance,
# MAGIC --     grp AS "group"
# MAGIC -- FROM final
# MAGIC -- ORDER BY group, r2_performance DESC;
# MAGIC
# MAGIC -- -------------------------------------------------------
# MAGIC -- -- 2️⃣ Summary check: distribution across groups
# MAGIC -- SELECT 
# MAGIC --     "group",
# MAGIC --     COUNT(customer_name) AS customer_count,
# MAGIC --     SUM(r2_total_count) AS total_count,
# MAGIC --     AVG(r2_performance) AS avg_performance
# MAGIC -- FROM analysts.idm_and_settlement.nhh_commercial_MHHS_Groups
# MAGIC -- GROUP BY "group"
# MAGIC -- ORDER BY "group";
# MAGIC
# MAGIC -- -------------------------------------------------------
# MAGIC -- -- 3️⃣ View full table
# MAGIC -- SELECT *
# MAGIC -- FROM analysts.idm_and_settlement.nhh_commercial_MHHS_Groups;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE analysts.idm_and_settlement.nhh_commercial_MHHS_Groups AS
# MAGIC -- 1️⃣ Base data
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         customer_name,
# MAGIC         meter_type,
# MAGIC         r2_volume_settled,
# MAGIC         r2_count_settled,
# MAGIC         r2_volume_error,
# MAGIC         r2_count_error,
# MAGIC         r2_total_volume,
# MAGIC         r2_total_count,
# MAGIC         r2_performance
# MAGIC     FROM analysts.idm_and_settlement.nhh_commercial_settlement_performance_with_us_r2
# MAGIC ),
# MAGIC
# MAGIC -- 2️⃣ Week start dates with weights
# MAGIC week_dates AS (
# MAGIC     SELECT
# MAGIC         week_start,
# MAGIC         ROW_NUMBER() OVER (ORDER BY week_start) AS week_number
# MAGIC     FROM (
# MAGIC         SELECT EXPLODE(SEQUENCE(
# MAGIC             TO_DATE('2026-10-12'),
# MAGIC             TO_DATE('2027-05-03'),
# MAGIC             INTERVAL 7 DAY
# MAGIC         )) AS week_start
# MAGIC     )
# MAGIC ),
# MAGIC
# MAGIC weights AS (
# MAGIC     SELECT *
# MAGIC     FROM VALUES
# MAGIC          (1,177),(2,290),(3,295),(4,295),(5,295),(6,635),
# MAGIC          (7,635),(8,635),(9,635),(10,200),(11,0),(12,0),
# MAGIC          (13,1250),(14,800),(15,800),(16,850),(17,900),(18,900),
# MAGIC          (19,1000),(20,1000),(21,900),(22,400),(23,300),(24,200),
# MAGIC          (25,100),(26,20),(27,22),(28,0),(29,0),(30,0)
# MAGIC     AS t(week_number, week_weight)
# MAGIC ),
# MAGIC
# MAGIC week_info AS (
# MAGIC     SELECT wd.week_start, w.week_number, w.week_weight
# MAGIC     FROM week_dates wd
# MAGIC     JOIN weights w ON wd.week_number = w.week_number
# MAGIC     WHERE w.week_weight > 0 -- ✅ only non-zero weeks
# MAGIC ),
# MAGIC
# MAGIC -- 3️⃣ Separate large and small customers
# MAGIC large_customers AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (ORDER BY r2_total_count DESC) AS rn_large
# MAGIC     FROM base
# MAGIC     WHERE r2_total_count > 0.1 * (SELECT SUM(r2_total_count) FROM base)
# MAGIC ),
# MAGIC
# MAGIC small_customers AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (ORDER BY r2_performance DESC) AS rn_small
# MAGIC     FROM base
# MAGIC     WHERE r2_total_count <= 0.1 * (SELECT SUM(r2_total_count) FROM base)
# MAGIC ),
# MAGIC
# MAGIC -- 4️⃣ Rank weeks by weight descending
# MAGIC week_ranks AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (ORDER BY week_weight DESC) AS rn_week
# MAGIC     FROM week_info
# MAGIC ),
# MAGIC
# MAGIC -- 5️⃣ Assign large customers to largest-weight weeks
# MAGIC large_assignment AS (
# MAGIC     SELECT
# MAGIC         c.*,
# MAGIC         w.week_start,
# MAGIC         w.week_number
# MAGIC     FROM large_customers c
# MAGIC     JOIN week_ranks w
# MAGIC       ON c.rn_large = w.rn_week
# MAGIC ),
# MAGIC
# MAGIC -- 6️⃣ Assign small customers round-robin over non-zero weeks
# MAGIC small_assignment AS (
# MAGIC     SELECT
# MAGIC         c.*,
# MAGIC         w.week_start,
# MAGIC         w.week_number
# MAGIC     FROM small_customers c
# MAGIC     CROSS JOIN week_info w
# MAGIC     WHERE ((c.rn_small - 1) % (SELECT COUNT(*) FROM week_info)) + 1 = w.week_number
# MAGIC ),
# MAGIC
# MAGIC -- 7️⃣ Combine large and small
# MAGIC final_assignment AS (
# MAGIC     SELECT * FROM large_assignment
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM small_assignment
# MAGIC )
# MAGIC
# MAGIC -- 8️⃣ Final output
# MAGIC SELECT *
# MAGIC FROM final_assignment
# MAGIC ORDER BY week_start, r2_performance DESC;
# MAGIC select * from analysts.idm_and_settlement.nhh_commercial_MHHS_Groups

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expanding groups out for commercial into mpan level

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE analysts.idm_and_settlement.nhh_commercial_MHHS_MPANs_R2 AS
# MAGIC WITH settlement_dates AS (
# MAGIC     SELECT run AS sd_run,
# MAGIC            MAX(date) AS settlement_date
# MAGIC     FROM analysts.idm_and_settlement.settlement_runs
# MAGIC     WHERE `DA Run Date` = (
# MAGIC         SELECT MIN(`DA Run Date`)
# MAGIC         FROM analysts.idm_and_settlement.settlement_runs
# MAGIC         WHERE `DA Run Date` >= CURRENT_DATE() + 1
# MAGIC     )
# MAGIC     GROUP BY run
# MAGIC ),
# MAGIC mpans AS (
# MAGIC     SELECT * 
# MAGIC     FROM analysts.idm_and_settlement.gsp
# MAGIC     WHERE affecting_settlement = true
# MAGIC       AND customer_type = 'Commercial'
# MAGIC       AND export_indicator IS false
# MAGIC       AND supply_end_date is null
# MAGIC ),
# MAGIC
# MAGIC -- replicate agg_r2 at MPAN level
# MAGIC R2 AS (
# MAGIC     SELECT customer_name, meter_type, mpan, 'error' AS r2_status
# MAGIC     FROM mpans
# MAGIC     WHERE run IN ('RF','R3','R2')
# MAGIC ),
# MAGIC
# MAGIC -- replicate agg_r2_s at MPAN level
# MAGIC R2_s AS (
# MAGIC     SELECT customer_name, meter_type, mpan, 'settled' AS r2_status
# MAGIC     FROM mpans
# MAGIC     LEFT JOIN settlement_dates 
# MAGIC         ON settlement_dates.sd_run = 'R2'
# MAGIC     WHERE run NOT IN ('RF', 'R3', 'R2') 
# MAGIC       AND supply_start_date <= settlement_date
# MAGIC       AND (supply_end_date >= settlement_date OR supply_end_date IS NULL)
# MAGIC ),
# MAGIC
# MAGIC -- union into "r2_total_count" MPAN set
# MAGIC all_r2_mpans AS (
# MAGIC     SELECT * FROM R2
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM R2_s
# MAGIC ),
# MAGIC
# MAGIC -- join to groups and add CED
# MAGIC mpans_with_groups AS (
# MAGIC     SELECT 
# MAGIC         a.customer_name,
# MAGIC         a.mpan,
# MAGIC         a.r2_status,
# MAGIC         g.week_start,
# MAGIC         c.CED
# MAGIC     FROM all_r2_mpans a
# MAGIC     LEFT JOIN analysts.idm_and_settlement.nhh_commercial_MHHS_Groups g
# MAGIC         ON a.customer_name = g.customer_name
# MAGIC        AND a.meter_type   = g.meter_type
# MAGIC     LEFT JOIN (
# MAGIC         SELECT Meterpoint AS mpan, MAX(CED) AS CED
# MAGIC         FROM analysts.business_growth.vw_fixed_contracts
# MAGIC         GROUP BY Meterpoint
# MAGIC     ) c
# MAGIC         ON a.mpan = c.mpan
# MAGIC )
# MAGIC -- final select with migration decision
# MAGIC SELECT
# MAGIC     c.*,
# MAGIC     CASE
# MAGIC         WHEN CED IS NULL THEN 'No CED – check manually'
# MAGIC         WHEN CED < DATE '2026-10-01' THEN 'Exclude – contract ended before migration'
# MAGIC         WHEN CED BETWEEN DATE '2026-10-01' AND DATE '2026-12-31' THEN 'Defer – expires too soon after migration start'
# MAGIC         WHEN CED BETWEEN DATE '2027-01-01' AND DATE '2027-02-28' THEN 'Migrate with caution – ends during migration'
# MAGIC         WHEN CED >= DATE '2027-03-01' THEN 'Safe – migrate in assigned group'
# MAGIC         ELSE 'Unknown'
# MAGIC     END AS migration_decision,
# MAGIC     m.DISTRIBUTOR AS DISTRIBUTOR,
# MAGIC     g.meter_type,
# MAGIC     g.eac
# MAGIC FROM mpans_with_groups c
# MAGIC  LEFT JOIN  source_systems.afms_elec_evo_dbo.mpan m
# MAGIC      ON c.mpan = m.J0003
# MAGIC  LEFT JOIN analysts.idm_and_settlement.gsp g
# MAGIC      ON c.mpan = g.mpan
# MAGIC ORDER BY week_start, customer_name, mpan;
# MAGIC
# MAGIC select * from analysts.idm_and_settlement.nhh_commercial_MHHS_MPANs_R2;

# COMMAND ----------

# MAGIC %md
# MAGIC #NHH Domestic

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting settlement performance for NHH Domestic

# COMMAND ----------

# DBTITLE 1,R2 NHH Domestic
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE analysts.idm_and_settlement.nhh_domestic_settlement_performance AS
# MAGIC -- settlement dates
# MAGIC with settlement_dates as (
# MAGIC   select run as sd_run, max(date) as settlement_date 
# MAGIC   from analysts.idm_and_settlement.settlement_runs
# MAGIC   where `DA Run Date` = current_date() +1
# MAGIC   group by run
# MAGIC ),
# MAGIC -- domestic mpans
# MAGIC mpans as (
# MAGIC   select * 
# MAGIC   from analysts.idm_and_settlement.gsp
# MAGIC   where affecting_settlement = true
# MAGIC     and customer_type IN ('Unknown', 'Domestic', 'Micro Business')
# MAGIC     and export_indicator is false
# MAGIC ),
# MAGIC -- not settled at RF
# MAGIC rf as (
# MAGIC   select * from mpans
# MAGIC   where run = 'RF'
# MAGIC ),
# MAGIC -- settled at RF
# MAGIC rf_s as (
# MAGIC   select m.*, settlement_date
# MAGIC   from mpans m
# MAGIC   left join settlement_dates s on s.sd_run = 'RF'
# MAGIC   where run <> 'RF'
# MAGIC     and supply_start_date <= settlement_date
# MAGIC     and (supply_end_date >= settlement_date or supply_end_date is null)
# MAGIC ),
# MAGIC -- rf results
# MAGIC rf_results as (
# MAGIC   select
# MAGIC     coalesce(rf_s.mpan, rf.mpan) as mpan,
# MAGIC     coalesce(rf_s.meter_type, rf.meter_type) as meter_type,
# MAGIC     coalesce(rf_s.eac, 0) as rf_volume_settled,
# MAGIC     case when rf_s.mpan is not null then 1 else 0 end as rf_count_settled,
# MAGIC     coalesce(rf.eac, 0) as rf_volume_error,
# MAGIC     case when rf.mpan is not null then 1 else 0 end as rf_count_error,
# MAGIC     coalesce(rf_s.eac, 0) + coalesce(rf.eac, 0) as rf_total_volume,
# MAGIC     (case when rf.mpan is not null then 1 else 0 end) 
# MAGIC       + (case when rf_s.mpan is not null then 1 else 0 end) as rf_total_count,
# MAGIC     coalesce(
# MAGIC       coalesce(rf_s.eac, 0) / nullif((coalesce(rf_s.eac, 0) + coalesce(rf.eac, 0)), 0), 
# MAGIC       0
# MAGIC     ) as rf_performance
# MAGIC   from rf_s
# MAGIC   full outer join rf
# MAGIC     on rf.mpan = rf_s.mpan
# MAGIC ),
# MAGIC -- repeat same pattern for R3, R2, R1, SF
# MAGIC r3 as (select * from mpans where run in ('RF','R3')),
# MAGIC r3_s as (
# MAGIC   select m.*, settlement_date
# MAGIC   from mpans m
# MAGIC   left join settlement_dates s on s.sd_run = 'R3'
# MAGIC   where run not in ('RF','R3')
# MAGIC     and supply_start_date <= settlement_date
# MAGIC     and (supply_end_date >= settlement_date or supply_end_date is null)
# MAGIC ),
# MAGIC r3_results as (
# MAGIC   select
# MAGIC     coalesce(r3_s.mpan, r3.mpan) as mpan,
# MAGIC     coalesce(r3_s.meter_type, r3.meter_type) as meter_type,
# MAGIC     coalesce(r3_s.eac, 0) as r3_volume_settled,
# MAGIC     case when r3_s.mpan is not null then 1 else 0 end as r3_count_settled,
# MAGIC     coalesce(r3.eac, 0) as r3_volume_error,
# MAGIC     case when r3.mpan is not null then 1 else 0 end as r3_count_error,
# MAGIC     coalesce(r3_s.eac, 0) + coalesce(r3.eac, 0) as r3_total_volume,
# MAGIC     (case when r3.mpan is not null then 1 else 0 end) 
# MAGIC       + (case when r3_s.mpan is not null then 1 else 0 end) as r3_total_count,
# MAGIC     coalesce(
# MAGIC       coalesce(r3_s.eac, 0) / nullif((coalesce(r3_s.eac, 0) + coalesce(r3.eac, 0)), 0), 
# MAGIC       0
# MAGIC     ) as r3_performance
# MAGIC   from r3_s
# MAGIC   full outer join r3
# MAGIC     on r3.mpan = r3_s.mpan
# MAGIC ),
# MAGIC -- repeat for R2
# MAGIC r2 as (select * from mpans where run in ('RF','R3','R2')),
# MAGIC r2_s as (
# MAGIC   select m.*, settlement_date
# MAGIC   from mpans m
# MAGIC   left join settlement_dates s on s.sd_run = 'R2'
# MAGIC   where run not in ('RF','R3','R2')
# MAGIC     and supply_start_date <= settlement_date
# MAGIC     and (supply_end_date >= settlement_date or supply_end_date is null)
# MAGIC ),
# MAGIC r2_results as (
# MAGIC   select
# MAGIC     coalesce(r2_s.mpan, r2.mpan) as mpan,
# MAGIC     coalesce(r2_s.meter_type, r2.meter_type) as meter_type,
# MAGIC     coalesce(r2_s.eac, 0) as r2_volume_settled,
# MAGIC     case when r2_s.mpan is not null then 1 else 0 end as r2_count_settled,
# MAGIC     coalesce(r2.eac, 0) as r2_volume_error,
# MAGIC     case when r2.mpan is not null then 1 else 0 end as r2_count_error,
# MAGIC     coalesce(r2_s.eac, 0) + coalesce(r2.eac, 0) as r2_total_volume,
# MAGIC     (case when r2.mpan is not null then 1 else 0 end) 
# MAGIC       + (case when r2_s.mpan is not null then 1 else 0 end) as r2_total_count,
# MAGIC     coalesce(
# MAGIC       coalesce(r2_s.eac, 0) / nullif((coalesce(r2_s.eac, 0) + coalesce(r2.eac, 0)), 0), 
# MAGIC       0
# MAGIC     ) as r2_performance
# MAGIC   from r2_s
# MAGIC   full outer join r2
# MAGIC     on r2.mpan = r2_s.mpan
# MAGIC ),
# MAGIC -- repeat for R1
# MAGIC r1 as (select * from mpans where run in ('RF','R3','R2','R1')),
# MAGIC r1_s as (
# MAGIC   select m.*, settlement_date
# MAGIC   from mpans m
# MAGIC   left join settlement_dates s on s.sd_run = 'R1'
# MAGIC   where run not in ('RF','R3','R2','R1')
# MAGIC     and supply_start_date <= settlement_date
# MAGIC     and (supply_end_date >= settlement_date or supply_end_date is null)
# MAGIC ),
# MAGIC r1_results as (
# MAGIC   select
# MAGIC     coalesce(r1_s.mpan, r1.mpan) as mpan,
# MAGIC     coalesce(r1_s.meter_type, r1.meter_type) as meter_type,
# MAGIC     coalesce(r1_s.eac, 0) as r1_volume_settled,
# MAGIC     case when r1_s.mpan is not null then 1 else 0 end as r1_count_settled,
# MAGIC     coalesce(r1.eac, 0) as r1_volume_error,
# MAGIC     case when r1.mpan is not null then 1 else 0 end as r1_count_error,
# MAGIC     coalesce(r1_s.eac, 0) + coalesce(r1.eac, 0) as r1_total_volume,
# MAGIC     (case when r1.mpan is not null then 1 else 0 end) 
# MAGIC       + (case when r1_s.mpan is not null then 1 else 0 end) as r1_total_count,
# MAGIC     coalesce(
# MAGIC       coalesce(r1_s.eac, 0) / nullif((coalesce(r1_s.eac, 0) + coalesce(r1.eac, 0)), 0), 
# MAGIC       0
# MAGIC     ) as r1_performance
# MAGIC   from r1_s
# MAGIC   full outer join r1
# MAGIC     on r1.mpan = r1_s.mpan
# MAGIC ),
# MAGIC -- repeat for SF
# MAGIC sf as (select * from mpans where run in ('RF','R3','R2','R1','SF')),
# MAGIC sf_s as (
# MAGIC   select m.*, settlement_date
# MAGIC   from mpans m
# MAGIC   left join settlement_dates s on s.sd_run = 'SF'
# MAGIC   where run not in ('RF','R3','R2','R1','SF')
# MAGIC     and supply_start_date <= settlement_date
# MAGIC     and (supply_end_date >= settlement_date or supply_end_date is null)
# MAGIC ),
# MAGIC sf_results as (
# MAGIC   select
# MAGIC     coalesce(sf_s.mpan, sf.mpan) as mpan,
# MAGIC     coalesce(sf_s.meter_type, sf.meter_type) as meter_type,
# MAGIC     coalesce(sf_s.eac, 0) as sf_volume_settled,
# MAGIC     case when sf_s.mpan is not null then 1 else 0 end as sf_count_settled,
# MAGIC     coalesce(sf.eac, 0) as sf_volume_error,
# MAGIC     case when sf.mpan is not null then 1 else 0 end as sf_count_error,
# MAGIC     coalesce(sf_s.eac, 0) + coalesce(sf.eac, 0) as sf_total_volume,
# MAGIC     (case when sf.mpan is not null then 1 else 0 end) 
# MAGIC       + (case when sf_s.mpan is not null then 1 else 0 end) as sf_total_count,
# MAGIC     coalesce(
# MAGIC       coalesce(sf_s.eac, 0) / nullif((coalesce(sf_s.eac, 0) + coalesce(sf.eac, 0)), 0), 
# MAGIC       0
# MAGIC     ) as sf_performance
# MAGIC   from sf_s
# MAGIC   full outer join sf
# MAGIC     on sf.mpan = sf_s.mpan
# MAGIC ),
# MAGIC -- collect master keys (mpans)
# MAGIC master_keys as (
# MAGIC   select mpan, meter_type from rf_results
# MAGIC   union
# MAGIC   select mpan, meter_type from r3_results
# MAGIC   union
# MAGIC   select mpan, meter_type from r2_results
# MAGIC   union
# MAGIC   select mpan, meter_type from r1_results
# MAGIC   union
# MAGIC   select mpan, meter_type from sf_results
# MAGIC )
# MAGIC select 
# MAGIC   master_keys.mpan,
# MAGIC   master_keys.meter_type,
# MAGIC   rf_results.* except(mpan,meter_type),
# MAGIC   r3_results.* except(mpan,meter_type),
# MAGIC   r2_results.* except(mpan,meter_type),
# MAGIC   r1_results.* except(mpan,meter_type),
# MAGIC   sf_results.* except(mpan,meter_type)
# MAGIC from master_keys
# MAGIC left join rf_results on rf_results.mpan = master_keys.mpan and rf_results.meter_type = master_keys.meter_type
# MAGIC left join r3_results on r3_results.mpan = master_keys.mpan and r3_results.meter_type = master_keys.meter_type
# MAGIC left join r2_results on r2_results.mpan = master_keys.mpan and r2_results.meter_type = master_keys.meter_type
# MAGIC left join r1_results on r1_results.mpan = master_keys.mpan and r1_results.meter_type = master_keys.meter_type
# MAGIC left join sf_results on sf_results.mpan = master_keys.mpan and sf_results.meter_type = master_keys.meter_type
# MAGIC order by rf_volume_error desc;
# MAGIC
# MAGIC select * from analysts.idm_and_settlement.nhh_domestic_settlement_performance

# COMMAND ----------

# MAGIC %md
# MAGIC ##Grouping NHH Domestic where we dont get HH Data into groups based on their R2 Performance
# MAGIC

# COMMAND ----------

# DBTITLE 1,nhh dom group r2
# MAGIC %sql
# MAGIC -- 1️⃣ Create the table with MHHS Groups including J0189
# MAGIC CREATE OR REPLACE TABLE analysts.idm_and_settlement.nhh_domestic_MHHS_Groups_r2 AS
# MAGIC
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         mpan,
# MAGIC         meter_type,
# MAGIC         r2_volume_settled,
# MAGIC         r2_count_settled,
# MAGIC         r2_volume_error,
# MAGIC         r2_count_error,
# MAGIC         r2_total_volume,
# MAGIC         r2_total_count,
# MAGIC         r2_performance
# MAGIC     FROM analysts.idm_and_settlement.nhh_domestic_settlement_performance
# MAGIC     WHERE mpan NOT IN (
# MAGIC         SELECT unit FROM refined.industry.smart_meter_half_hourly_readings
# MAGIC     )
# MAGIC ),
# MAGIC total_mpan AS (
# MAGIC     SELECT SUM(r2_total_count) AS total_mpan
# MAGIC     FROM base
# MAGIC ),
# MAGIC group1_candidates AS (
# MAGIC     SELECT *,
# MAGIC         CASE
# MAGIC             WHEN r2_performance = 1 THEN 'best'
# MAGIC             WHEN r2_performance > 0.7 THEN 'good'
# MAGIC             WHEN r2_performance > 0.3 THEN 'alright'
# MAGIC             ELSE 'poor'
# MAGIC         END AS perf_bucket
# MAGIC     FROM base
# MAGIC ),
# MAGIC group1_ordered AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY perf_bucket ORDER BY mpan DESC) AS rn
# MAGIC     FROM group1_candidates
# MAGIC ),
# MAGIC group1_cumulative AS (
# MAGIC     SELECT g.*,
# MAGIC         SUM(r2_total_count) OVER (PARTITION BY perf_bucket ORDER BY rn) AS cum_count
# MAGIC     FROM group1_ordered g
# MAGIC ),
# MAGIC group1 AS (
# MAGIC     SELECT g.*,
# MAGIC         1 AS grp
# MAGIC     FROM group1_cumulative g
# MAGIC     CROSS JOIN total_mpan t
# MAGIC     WHERE (perf_bucket = 'best' AND cum_count <= 0.056 * t.total_mpan)
# MAGIC        OR (perf_bucket = 'good' AND cum_count <= 0.02 * t.total_mpan)
# MAGIC        OR (perf_bucket = 'alright' AND cum_count <= 0.01 * t.total_mpan)
# MAGIC        OR (perf_bucket = 'poor' AND cum_count <= 0.014 * t.total_mpan)
# MAGIC ),
# MAGIC remaining AS (
# MAGIC     SELECT b.*
# MAGIC     FROM base b
# MAGIC     LEFT JOIN group1 g
# MAGIC         ON b.mpan = g.mpan
# MAGIC        AND b.meter_type = g.meter_type
# MAGIC     WHERE g.mpan IS NULL
# MAGIC ),
# MAGIC banded AS (
# MAGIC     SELECT
# MAGIC         r.*,
# MAGIC         NTILE(9) OVER (ORDER BY r2_performance DESC) AS performance_band
# MAGIC     FROM remaining r
# MAGIC ),
# MAGIC band_with_order_key AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE 
# MAGIC             WHEN performance_band % 2 = 1 THEN -r2_total_count
# MAGIC             ELSE r2_total_count
# MAGIC         END AS order_key
# MAGIC     FROM banded
# MAGIC ),
# MAGIC band_positioned AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY performance_band
# MAGIC             ORDER BY order_key
# MAGIC         ) AS band_position
# MAGIC     FROM band_with_order_key
# MAGIC ),
# MAGIC assigned_groups AS (
# MAGIC     SELECT
# MAGIC         mpan,
# MAGIC         meter_type,
# MAGIC         r2_volume_settled,
# MAGIC         r2_count_settled,
# MAGIC         r2_volume_error,
# MAGIC         r2_count_error,
# MAGIC         r2_total_volume,
# MAGIC         r2_total_count,
# MAGIC         r2_performance,
# MAGIC         ((band_position - 1) % 9) + 2 AS grp
# MAGIC     FROM band_positioned
# MAGIC ),
# MAGIC final AS (
# MAGIC     SELECT
# MAGIC         mpan,
# MAGIC         meter_type,
# MAGIC         r2_volume_settled,
# MAGIC         r2_count_settled,
# MAGIC         r2_volume_error,
# MAGIC         r2_count_error,
# MAGIC         r2_total_volume,
# MAGIC         r2_total_count,
# MAGIC         r2_performance,
# MAGIC         grp
# MAGIC     FROM group1
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC         mpan,
# MAGIC         meter_type,
# MAGIC         r2_volume_settled,
# MAGIC         r2_count_settled,
# MAGIC         r2_volume_error,
# MAGIC         r2_count_error,
# MAGIC         r2_total_volume,
# MAGIC         r2_total_count,
# MAGIC         r2_performance,
# MAGIC         grp
# MAGIC     FROM assigned_groups
# MAGIC )
# MAGIC
# MAGIC -- Materialize the table with J0189
# MAGIC SELECT
# MAGIC     f.*,
# MAGIC     m.DISTRIBUTOR
# MAGIC FROM final f
# MAGIC LEFT JOIN source_systems.afms_elec_evo_dbo.mpan m
# MAGIC     ON f.mpan = m.J0003
# MAGIC ORDER BY grp, r2_performance DESC;
# MAGIC
# MAGIC -------------------------------------------------------
# MAGIC -- 2️⃣ Summary check: distribution across groups
# MAGIC SELECT 
# MAGIC     grp, 
# MAGIC     COUNT(mpan) AS mpan_count, 
# MAGIC     SUM(r2_total_count) AS total_count, 
# MAGIC     AVG(r2_performance) AS avg_performance
# MAGIC FROM analysts.idm_and_settlement.nhh_domestic_MHHS_Groups_r2
# MAGIC GROUP BY grp
# MAGIC ORDER BY grp;
# MAGIC
# MAGIC -------------------------------------------------------
# MAGIC --3️⃣ View full table
# MAGIC SELECT * 
# MAGIC FROM analysts.idm_and_settlement.nhh_domestic_MHHS_Groups_r2;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping NHH Domestic where we get HH Data into groups for migration based on whether they are loss making

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace the table in your schema
# MAGIC CREATE OR REPLACE TABLE analysts.idm_and_settlement.mpan_profit__mhhs_dom_groups AS
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         er.mpan,
# MAGIC         (er.hh_settle_cost - er.profiled_cost) AS profitability,
# MAGIC         gsp.meter_type,
# MAGIC         dist.DISTRIBUTOR
# MAGIC     FROM analysts.pricing_reporting.elective_results er
# MAGIC     INNER JOIN analysts.idm_and_settlement.gsp gsp
# MAGIC         ON er.mpan = gsp.mpan
# MAGIC     LEFT JOIN source_systems.afms_elec_evo_dbo.mpan dist
# MAGIC         ON er.mpan = dist.J0003
# MAGIC     WHERE er.profiled_cost IS NOT NULL
# MAGIC       AND gsp.customer_type = 'Domestic'
# MAGIC ),
# MAGIC
# MAGIC banded AS (
# MAGIC     SELECT
# MAGIC         b.*,
# MAGIC         NTILE(10) OVER (ORDER BY profitability DESC) AS performance_band
# MAGIC     FROM base b
# MAGIC ),
# MAGIC
# MAGIC band_with_order_key AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE 
# MAGIC             WHEN performance_band % 2 = 1 THEN -profitability
# MAGIC             ELSE profitability
# MAGIC         END AS order_key
# MAGIC     FROM banded
# MAGIC ),
# MAGIC
# MAGIC band_positioned AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY performance_band
# MAGIC             ORDER BY order_key
# MAGIC         ) AS band_position
# MAGIC     FROM band_with_order_key
# MAGIC ),
# MAGIC
# MAGIC assigned_groups AS (
# MAGIC     SELECT
# MAGIC         mpan,
# MAGIC         profitability,
# MAGIC         meter_type,
# MAGIC         distributor,
# MAGIC         ((band_position - 1) % 10) + 1 AS grp
# MAGIC     FROM band_positioned
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM assigned_groups;
# MAGIC select * from analysts.idm_and_settlement.mpan_profit__mhhs_dom_groups;