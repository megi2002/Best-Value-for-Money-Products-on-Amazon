file_path = "/data/doina/Amazon/reviews_CDs_and_Vinyl.json.gz"
reviews_df = spark.read.json(file_path)
reviews_df.printSchema()
reviews_df = reviews_df.select("reviewText", "overall")
reviews_df.show(5, truncate=False)
high_value_reviews_df = reviews_df.filter(reviews_df["overall"] >= 4)
high_value_reviews_df.show(5, truncate=False)
from pyspark.sql.functions import lower, regexp_replace, split

cleaned_df = high_value_reviews_df.withColumn(
"reviewText",
 regexp_replace(lower(high_value_reviews_df["reviewText"]), "[^a-zA-Z\s]", "")  # Remove punctuation, make lowercase).withColumn("tokens",split("reviewText", "\s+")  # Tokenize text by splitting on whitespace )
cleaned_df.show(5, truncate=False)
from pyspark.sql.functions import expr


stopwords = ["the", "is", "in", "and", "of", "to", "a", "for", "on", "with", "at", "by", "an"]


stopwords_array = ", ".join([f"'{word}'" for word in stopwords])


filtered_df = cleaned_df.withColumn("filtered_tokens",expr(f"filter(tokens, x -> NOT(x IN ({stopwords_array})))") )

filtered_df.show(5, truncate=False)
from pyspark.sql.functions import explode, col

exploded_df = filtered_df.select(explode(col("filtered_tokens")).alias("word"))


word_counts = exploded_df.groupBy("word").count().orderBy(col("count").desc())

# Show the most common words
word_counts.show(20, truncate=False)


from pyspark.sql.functions import col, lower, regexp_replace, split, explode, log, size, percent_rank
from pyspark.sql.window import Window

meta_file_path = "/data/doina/Amazon/meta_CDs_and_Vinyl.json.gz"
 meta_df = spark.read.json(meta_file_path)
 = meta_df.withColumn("WVFM_Ratio", (log(col("num_related") + 1)) / col("price"))

window_spec = Window.orderBy(col("WVFM_Ratio").desc())
meta_df = meta_df.withColumn("percentile", percent_rank().over(window_spec))

df_top_50 = meta_df.filter(col("percentile") <= 0.5)


df_top_50.write.csv("/user/s3441261/top_value_for_money_products", header=True, mode="overwrite")
df_top_50_asin = df_top_50.select("asin")

file_path = "/data/doina/Amazon/reviews_CDs_and_Vinyl.json.gz"
reviews_df = spark.read.json(file_path)

reviews_filtered = reviews_df.join(df_top_50_asin, "asin", "inner")

reviews_cleaned = reviews_filtered.withColumn("clean_review",
    regexp_replace(lower(col("reviewText")), "[^a-zA-Z\s]", "")).withColumn(
    "tokens", split(col("clean_review"), "\s+"))

exploded_words = reviews_cleaned.select("asin", explode(col("tokens")).alias("word"))

aspects = ["price", "quality", "features", "design", "value", "performance", "functionality", "durability"]

wvfm_aspect_counts = exploded_words.filter(col("word").isin(aspects)).groupBy("word").count()

wvfm_aspect_counts.show()
