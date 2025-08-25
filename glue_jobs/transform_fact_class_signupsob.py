import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark & Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ✅ Required for Requester Pays + Region
hadoopConf = spark._jsc.hadoopConfiguration()


# ✅ Read the full CSV from the shared bucket
df = spark.read.option("header", "true") \
    .csv("s3a://ziyiyan-peakfitness/input/mindbody_schedule.csv")

# ✅ Select and rename fields
df_clean = df.selectExpr(
    "session_id",
    "datetime",
    "class_code",
    "class_name",
    "instructor_id",
    "instructor_name",
    "loc_id",
    "city",
    "location_name as room",
    "year", "month", "week_number as week",
    "day_of_week",
    "is_weekend",
    "duration_mins"
)

# ✅ Write the cleaned data to your own bucket in Parquet format
df_clean.write.mode("overwrite").parquet("s3://ziyiyan-peakfitness/peak-fitness-transformed/fact_class_signups/")

# ✅ Finish the Glue job
job.commit()
