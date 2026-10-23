from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

# 1. Création de la session Spark avec les identifiants MinIO et les dépendances Kafka/S3
spark = SparkSession.builder \
    .appName("Lakehouse-CDC-Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("🚀 Streaming PySpark démarré : Écoute de Kafka et écriture dans MinIO...")

# 2. Lecture du flux Kafka en temps réel (Debezium)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "lakehouse_cdc.public.*") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Nettoyage du JSON Debezium (On extrait uniquement la donnée 'after')
parsed_df = df.selectExpr("CAST(value AS STRING) as json_payload") \
    .withColumn("data", get_json_object(col("json_payload"), "$.payload.after")) \
    .filter(col("data").isNotNull())

# 4. Écriture du flux en Parquet dans le bucket MinIO
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://lakehouse/clients/") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/clients/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
