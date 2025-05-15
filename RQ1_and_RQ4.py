from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, avg, count, log, when


spark = SparkSession.builder.appName("ValueForMoney").getOrCreate()

products = spark.read.json("/data/doina/Amazon/meta_CDs_and_Vinyl.json.gz")

reviews = spark.read.json("/data/doina/Amazon/reviews_CDs_and_Vinyl.json.gz")

review_metrics = reviews.groupBy("asin").agg(avg("overall").alias("average_rating"), count("overall").alias("number_of_reviews"))

products_filtered= products.select("asin", "title", "price").filter((col("price").isNotNull()) & (col("price")>0))

combined= products_filtered.join(review_metrics, "asin", "inner")

result = combined.withColumn("value_for_money_ratio", (col("average_rating") * log(col("number_of_reviews")+ 1)) / col("price"))

top_products = result.orderBy(col("value_for_money_ratio").desc())

top_products.select("title", "average_rating", "number_of_reviews", "value_for_money_ratio").show(10, truncate=False)

spark.stop()