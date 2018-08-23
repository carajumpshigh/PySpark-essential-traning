# Read text file
path = "/databricks-datasets/bikeSharing/README.md"
data = sc.textFile(path) # use the sc context to read in a text file
data.count()

# Print the lines
data.first()
data.take(20)

# Read files and cache data
logFile = path
logData = sc.textFile(logFile).cache()

# Get number of times "car" shows up
numCars = logData.filter(lamda s: 'car' in s.lower()).count()
print("Lines with 'car': %i" % (numCars))

# Import csv files
path = "/data-001/csv/datasets/*.csv"
files = sc.wholeTextFiles(path)   # get each file separately
files.count()

# Convert files to dataframe
filenames = files.toDF(['name','data'])
display(filenames)
display(filenames.select('name'))

#=====================================================================================

# Load csv data as text
path = "/data-001/data.csv"
data = spark.read.csv(path)
data.take(20)   # show sample

# Read in data to dataframe with column
df = spark.read.load(path,
                    format='com.databricks.spark.csv',
                    header='true',
                    inferSchema='true')
display(df)   # show sample

#======================================================================================

# Select one column
df.select("Country").show()

# Show distinct results and sort
display(df.select("Country")  # choose one column
          .distinct()   # remove depulicates
          .orderBy("Country")   # sort results in asc
)

# Calculate
display(
  df
    .select(df["InvoiceNo"],df["UnitPrice"]*df["Quantity"])
    .groupBy("InvoiceNo")
    .sum()
  )
 
 # Filter
 df.filter(df["InvoiceNo"]==536596).show()
 
 # Query case
 # Top 10 products in the UK
 display(
  df
    .select(df["Country"], df["Description"],(df["UnitPrice"]*df["Quantity"]).alias("Total"))
    .groupBy("Country", "Description")
    .sum()
    .filter(df["Country"]=="United Kingdom")
    .sort("sum(Total)", ascending=False)
    .limit(10)
  )
  
  # Calculate product sales by country
  rl = df.select(df["Country"], df["Description"],(df["UnitPrice"]*df["Quantity"]).alias("Total"))
  display(rl)
  
  # Save results as table
  rl.write.saveAsTable("product_sales_by_country")
