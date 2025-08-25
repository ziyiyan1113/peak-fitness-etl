import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load class signup data
df = spark.read.parquet("s3://ziyiyan-peakfitness/peak-fitness-transformed/fact_class_signups/")

# Group by user + location and count number of signups
signup_counts = df.groupBy("instructor_id", "loc_id").agg(
    count("session_id").alias("class_count")
)

# Use window function to rank top instructors per location
windowSpec = Window.partitionBy("loc_id").orderBy(col("class_count").desc())

ranked = signup_counts.withColumn("rank", row_number().over(windowSpec))

# Keep top 5 per location
top_attendees = ranked.filter(col("rank") <= 5)

# Save leaderboard as CSV
top_attendees.write.mode("overwrite").option("header", "true") \
    .csv("s3://ziyiyan-peakfitness/leaderboards/top_attendees_by_location.csv")

job.commit()
