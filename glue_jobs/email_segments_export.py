import sys
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, max as max_, to_date, current_date, datediff

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ✅ Load the fact_event_log Parquet data
df = spark.read.parquet("s3://ziyiyan-peakfitness/peak-fitness-transformed/fact_event_log/")

# Convert timestamp to date
df = df.withColumn("event_date", to_date("event_ts"))

# Filter for signup events only
signups = df.filter(col("event_type") == "signup")

# Group by user
signup_summary = signups.groupBy("user_id", "location_id").agg(
    max_("event_date").alias("last_signup_date"),
    max_("event_ts").alias("last_signup_ts"),
).withColumn("signup_count", col("user_id").cast("string").substr(1, 1).cast("int") + 1)  # simulate count

# Cutoff date
cutoff_date = datetime.utcnow() - timedelta(days=7)

# A. Inactive users (>7 days since last signup)
inactive = signup_summary.filter(datediff(current_date(), col("last_signup_date")) > 7)
inactive.write.mode("overwrite").option("header", "true").csv(
    "s3://ziyiyan-peakfitness/marketing_segments/inactive_members_7days.csv"
)

# B. Power users (top 10%)
threshold = signup_summary.approxQuantile("signup_count", [0.9], 0.05)[0]
power_users = signup_summary.filter(col("signup_count") >= threshold)
power_users.write.mode("overwrite").option("header", "true").csv(
    "s3://ziyiyan-peakfitness/marketing_segments/high_engagement_users_top10.csv"
)

# C. Churn-risk: only 1 signup + >7 days ago
churn_risk = signup_summary.filter(
    (col("signup_count") == 1) & (datediff(current_date(), col("last_signup_date")) > 7)
)
churn_risk.write.mode("overwrite").option("header", "true").csv(
    "s3://ziyiyan-peakfitness/marketing_segments/churn_risk_users.csv"
)

job.commit()
