from pyspark.sql import SparkSession

# Tạo một phiên Spark
spark = SparkSession.builder \
    .appName("Count Columns in CSV") \
    .getOrCreate()

# Đọc tệp CSV từ HDFS
df = spark.read.csv("hdfs://localhost:9870/batch-layer/raw_data.csv", header=True, inferSchema=True)

# Lấy danh sách các cột
columns = df.columns
num_columns = len(columns)

# In số lượng cột và tên cột
print(f"Number of columns: {num_columns}")
print("Column names:")
for column in columns:
    print(column)
