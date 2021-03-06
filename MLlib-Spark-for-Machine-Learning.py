# Prepare data

# Use the Spark CSV datasource with options specifying:
# - First line of file is a header
# - Automatically infer the schema of the data

path = '/bulabula/CogsleyServices-SalesData-US.csv'
data = sqlContext.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load(path)
  
data.cache() # Cache data for faster reuse
data = data.dropna() # drop rows with missing values
 
# Register table so it is accessible via SQL Context
# For Apache Spark = 2.0
# data.createOrReplaceTempView("data_geo")

display(data)

# Aggregate and convert
# Get monthly sales totals
summary = data.select("OrderMonthYear", "SaleAmount")\
              .groupBy("OrderMonthYear")\
              .sum()\
              .orderBy("OrderMonthYear")\
              .toDF("OrderMonthYear","SaleAmount")

# Convert OrderMonthYear to integer type
results = summary.map(lambda r: (int(r.OrderMonthYear.replace('-','')), r.SaleAmount))\
                 .toDF(["OrderMonthYear","SaleAmount"])

# Convert df to features and label
from pyspark.mllib.regression import LabeledPoint

data = results.select("OrderMonthYear", "SaleAmount")\
  .map(lambda r: LabeledPoint(r[1],[r[o]]))\
  .toDF()
 
display(data)

#======================================================================================

# Build a linear regression model

# Import package and define model
from pyspark.ml.regression import LinearRegression
lr = LinearRegression()

# Fit
modelA = lr.fit(data, {lr.regParam:0.0})  # regParam to set parameter
modelB = lr.fit(data, {lr.regParam:100.0})

# Predict
predictionsA = modelA.transform(data)
predictionsA = modelB.transform(data)
display(predictionsA)

# Evaluate
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(metricName="rmse")

RMSE = evaluator.evaluate(predictionsA)
print("ModelA: Root Mean Squared Error = " + str(RMSE))

RMSE = evaluator.evaluate(predictionsB)
print("ModelB: Root Mean Squared Error = " + str(RMSE))

# Export results to table
cols = ["OrderMonthYear", "SaleAmount", "Prediction"]

# Use parallelize to create an RDD
# Use map() with lambda to parse features
tableA = sc.parallelize(\
            predictionsA.map(lambda r: (float(r.features[0]), r.label, r.prediction)).collect()\
         ).toDF(cols) 
         
tableB = sc.parallelize(\
            predictionsB.map(lambda r: (float(r.features[0]), r.label, r.prediction)).collect()\
         ).toDF(cols) 

display(tableA)

# Save results as tables
tableA.write.saveAsTable('predictionsA', mode='overwrite')
print "Created predictionsA table"

tableB.write.saveAsTable('predictionsB', mode='overwrite')
print "Created predictionsB table"
