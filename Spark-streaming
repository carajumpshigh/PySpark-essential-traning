# Load data
from pyspark.sql.types import *

path = "file:/databricks/driver/sales_log/"

# Create schema for data so stream processing is faster
salesSchema = StructType([
  StructField("OrderID", DoubleType(), True),
  StructField("OrderDate", StringType(), True),
  StructField("Quantity", DoubleType(), True),
  StructField("DiscountPct", DoubleType(), True),
  StructField("Rate", DoubleType(), True),
  StructField("SaleAmount", DoubleType(), True),
  StructField("CustomerName", StringType(), True),
  StructField("State", StringType(), True),
  StructField("Region", StringType(), True),
  StructField("ProductKey", StringType(), True),
  StructField("RowCount", DoubleType(), True),
  StructField("ProfitMargin", DoubleType(), True)])

# Static DataFrame containing all the files in sales_log
data = (
  spark
    .read
    .schema(salesSchema)
    .csv(path)
)


# create table so we can use SQL
data.createOrReplaceTempView("sales")

display(data)

# SQL...

# Streaming setup
# Similar to definition of staticInputDF above, just using `readStream` instead of `read`
streamingInputDF = (
  spark
    .readStream                       
    .schema(salesSchema)              # Set the schema for our feed
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .csv(path)
)

# Same query as staticInputDF
streamingCountsDF = (                 
  streamingInputDF
    .select("ProductKey", "SaleAmount")
    .groupBy("ProductKey")
    .sum()
)

streamingCountsDF.isStreaming

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("sales_stream")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)
