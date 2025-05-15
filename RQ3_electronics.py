file_path = "/data/doina/Amazon/reviews_Electronics.json.gz"


reviews_df = spark.read.json(file_path)


reviews_df = reviews_df.select("reviewText", "overall")


high_value_reviews_df = reviews_df.filter(col("overall") >= 4)


cleaned_df = high_value_reviews_df.withColumn(
    "reviewText",
    regexp_replace(lower(col("reviewText")), "[^a-zA-Z\s]", "") 
).withColumn(
    "tokens",
    split(col("reviewText"), "\s+")
)


stopwords_remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")
filtered_df = stopwords_remover.transform(cleaned_df)

exploded_df = filtered_df.select(explode(col("filtered_tokens")).alias("word"))


word_counts = exploded_df.groupBy("word").count().orderBy(col("count").desc())


aspects = ["durability", "functionality", "quality", "value", "price", "design", "performance", "features"]
aspect_counts = word_counts.filter(col("word").isin(aspects))

aspect_counts.show()

output_path = "/data/doina/Amazon/analysis_results"
aspect_counts.write.csv(output_path, header=True)

reviews_path = "/data/doina/Amazon/reviews_Electronics.json.gz" 
reviews_df = spark.read.json(reviews_path)
reviews_df = reviews_df.select("asin", "reviewText", "overall").dropna() 

df_top_50_asin = df_top_50.select("asin") 

reviews_filtered = reviews_df.join(df_top_50_asin, "asin", "inner") 

reviews_filtered.show(5, truncate=False)

from pyspark.sql.functions import lower, regexp_replace, split, explode, col 

reviews_cleaned = reviews_filtered.withColumn( "clean_review", regexp_replace(lower(col("reviewText")), "[^a-zA-Z\s]", "")).withColumn( "tokens", split(col("clean_review"), "\s+") # Tokenize into words ) 
exploded_words = reviews_cleaned.select("asin", "overall", explode(col("tokens")).alias("word")) aspects = ["price", "quality", "features", "design", "value", "performance", "functionality", "durability"] 
word_counts = exploded_words.filter(col("word").isin(aspects)).groupBy("word").count() word_counts.show()


reviews_filtered = reviews_filtered.withColumn("sentiment", 
    col("overall").cast("double")
).withColumn(
    "sentiment", 
    col("sentiment").between(4, 5) )


exploded_words_sentiment = reviews_filtered.withColumn(
    "tokens", split(lower(col("reviewText")), "\s+")
).select("asin", "sentiment", explode(col("tokens")).alias("word"))

word_sentiment_counts = exploded_words_sentiment.filter(col("word").isin(aspects)) \
    .groupBy("word", "sentiment").count().orderBy("word", "sentiment")

word_sentiment_counts.show()
