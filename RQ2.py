from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log, count, avg

spark = SparkSession.builder.appName("ValueForMoney").getOrCreate()

def find_percentage_good_value(meta_file,review_file):
    product_df = spark.read.json(meta_file)
    reviews_df = spark.read.json(review_file)
    review_agg=reviews_df.groupBy("asin").agg(avg("overall").alias("avg_rating"),count("overall").alias("num_reviews"))
    product_with_reviews = product_df.join(review_agg, "asin", "left")
    # attribute_names = [field.name for field in product_with_reviews.schema.fields]
    product_with_vfm = product_with_reviews.withColumn("value_for_money",(col("avg_rating") * log(col("num_reviews") + 1)) / col("price"))
    percentile_threshold = product_with_vfm.approxQuantile("value_for_money", [0.5], 0.05)[0]
    high_rated_products = product_with_vfm.filter(col("avg_rating") >= 4.5)
    good_value_products = high_rated_products.filter(col("value_for_money") >= percentile_threshold)
    total_high_rated = high_rated_products.count()
    total_good_value = good_value_products.count()
    percentage_good_value = (total_good_value / total_high_rated) * 100 if total_high_rated > 0 else 0
    return percentage_good_value

elecronic_product="/data/doina/Amazon/meta_Electronics.json.gz"
electronic_reviews="/data/doina/Amazon/reviews_Electronics.json.gz"
cd_vinyl_products="/data/doina/Amazon/meta_CDs_and_Vinyl.json.gz"
cd_vinyl_reviews="/data/doina/Amazon/reviews_CDs_and_Vinyl.json.gz"

print(find_percentage_good_value(elecronic_product,electronic_reviews))
print(find_percentage_good_value(cd_vinyl_products,cd_vinyl_reviews))