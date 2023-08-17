#Best Menus

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("UberEatsAnalysis") \
    .getOrCreate()

# Load data from your database into DataFrames
restaurants = spark.read.csv("gs://dataproc-staging-us-central1-418685873277-utpempl8/pyspark_nlp/data/restaurants.csv", header=True, inferSchema=True)
restaurant_menus = spark.read.csv("gs://dataproc-staging-us-central1-418685873277-utpempl8/pyspark_nlp/data/restaurant_menus.csv", header=True, inferSchema=True)

# Join the two DataFrames based on restaurant_id
joined_df = restaurants.alias("r").join(restaurant_menus.alias("m"), restaurants.id == restaurant_menus.id)

# Perform aggregation
best_menus_df = joined_df.groupBy("m.id", "m.name").agg(
    avg("r.score").alias("average_score"),
    count("r.ratings").alias("total_ratings")
)

# Order the results
ordered_menus_df = best_menus_df.orderBy(
    best_menus_df["average_score"].desc(), best_menus_df["total_ratings"].desc()
)

# Show the results
ordered_menus_df.show(10)  # You can adjust the number of results to display

# Stop the Spark session
spark.stop()
