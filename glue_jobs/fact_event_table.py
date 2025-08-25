import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Enable requester pays
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.requester.pays.enabled", "true")

# ✅ Step 1: Recreate fact_event_log
df = spark.read.json("s3a://peak-fitness-data-raw/peak-fitness-stream/non-pii/events/*.json")
df_filtered = df.filter(df.event_type == "signup")
df_filtered.write.mode("overwrite").parquet("s3://ziyiyan-peakfitness/peak-fitness-transformed/fact_event_log/")

# ✅ Step 2: Load both source datasets
df_class = spark.read.parquet("s3://ziyiyan-peakfitness/peak-fitness-transformed/fact_class_signups/")
df_event = spark.read.parquet("s3://ziyiyan-peakfitness/peak-fitness-transformed/fact_event_log/")

# ✅ Step 3: Join on session_id
signup_events = df_event.filter(col("event_type") == "signup")
joined_df = signup_events.join(df_class, on="session_id", how="inner")

# ✅ Step 4: Select clean columns
final_df = joined_df.select(
    "user_id",
    "session_id",
    "datetime",
    "class_code",
    "class_name",
    "instructor_id",
    "instructor_name",
    "loc_id",
    "city",
    "room",
    "year",
    "month",
    "week",
    "day_of_week",
    "is_weekend",
    "duration_mins"
)

# ✅ Step 5: Write to new output folder
final_df.write.mode("overwrite").parquet(
    "s3://ziyiyan-peakfitness/peak-fitness-transformed/fact_class_signups_with_user_id/"
)

job.commit()
