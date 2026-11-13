from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------- Start Spark ----------
spark = (
    SparkSession.builder
    .appName("ReadWriteS3")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .getOrCreate()
)

# ---------- 1. Read ----------
df = spark.read.csv("s3a://demo-divithraju/demo/employee.csv", header=True, inferSchema=True)
print("Raw data:", df.count(), "rows")
df.show()

# ---------- 2. Clean ----------
df = df.dropDuplicates()                          # remove duplicate rows
df = df.dropna()                                  # remove rows with missing values
df = df.withColumn("name", F.initcap(F.trim("name")))              # John, Alice ...
df = df.withColumn("city", F.initcap(F.trim("city")))              # Chennai, Pune ...
df = df.withColumn("department", F.trim("department"))

# ---------- 3. Validate ----------
df = df.filter(F.col("salary") > 0)               # salary must be positive
print("Clean data:", df.count(), "rows")
print("Duplicate ids:", df.count() - df.select("id").distinct().count())

# ---------- 4. Transform ----------
df = df.withColumn("bonus", F.col("salary") * 0.10)                # 10% bonus
df = df.withColumn(
    "salary_level",
    F.when(F.col("salary") < 50000, "Low")
     .when(F.col("salary") < 70000, "Medium")
     .otherwise("High")
)
df.show()

# ---------- 5. Aggregate ----------
department_summary = df.groupBy("department").agg(
    F.count("id").alias("employees"),
    F.sum("salary").alias("total_salary"),
    F.round(F.avg("salary"), 2).alias("avg_salary"),
    F.max("salary").alias("max_salary"),
)
department_summary.show()

# ---------- 6. Write ----------
df.write.mode("overwrite").parquet("s3a://silver-bucketdivith/demo/")
department_summary.write.mode("overwrite").parquet("s3a://silver-bucketdivith/gold/department_summary/")
print("Done!")

spark.stop()
