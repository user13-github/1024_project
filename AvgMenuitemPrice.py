# Calculating the average menu item price for each restaurant category.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, trim


    # Initialize SparkSession
spark = SparkSession.builder \
    .appName("AverageMenuItemPrice") \
    .getOrCreate()

# Load data from your database into DataFrames
restaurants = spark.read.csv("gs://dataproc-staging-us-central1-418685873277-utpempl8/pyspark_nlp/data/restaurants.csv", header=True, inferSchema=True)
restaurant_menus = spark.read.csv("gs://dataproc-staging-us-central1-418685873277-utpempl8/pyspark_nlp/data/restaurant_menus.csv", header=True, inferSchema=True)


# Join the two DataFrames based on restaurant_id
joined_df = restaurants.alias("r").join(restaurant_menus.alias("m"), restaurants.id == restaurant_menus.id)


# Calculate average menu item price for each restaurant category
avg_price_per_category = joined_df.groupBy("r.category").agg(avg("m.price").alias("avg_price"))\
.orderBy("avg_price", ascending=False)

# Show the results
avg_price_per_category.show()

# Stop the SparkSession
spark.stop()


