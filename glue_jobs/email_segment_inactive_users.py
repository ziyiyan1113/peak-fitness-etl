import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, max as max_, current_date, to_date, datediff

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load event log data
df = spark.read.parquet("s3://ziyiyan-peakfitness/peak-fitness-transformed/fact_event_log/")

# Parse timestamp to date
df = df.withColumn("event_date", to_date("event_ts"))

# Get latest signup date per user
last_signup = df.filter(col("event_type") == "signup") \
    .groupBy("user_id", "location_id") \
    .agg(max_("event_date").alias("last_signup_date"))

# Filter users inactive for 7+ days
inactive_users = last_signup.filter(datediff(current_date(), col("last_signup_date")) > 7)

# Write to CSV
inactive_users.write.mode("overwrite").option("header", "true") \
    .csv("s3://ziyiyan-peakfitness/marketing_segments/inactive_members.csv")

job.commit()
