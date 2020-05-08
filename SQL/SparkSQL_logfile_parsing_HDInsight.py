
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql.functions import sum
from pyspark.sql.types import IntegerType
import timeit

application_name = "SparkSQLProg"
# Tell master to use YARN.
master = "yarn-client"

# configure executors and cores
num_executors = 5
num_cores = 2

conf = SparkConf()
conf.set("spark.app.name", application_name)
conf.set("spark.master", master)
conf.set("spark.executor.cores", num_cores)
conf.set("spark.executor.instances", num_executors);

# Create Spark Session based off above configuration
spark = SparkSession.builder.config(conf=conf).appName(application_name).getOrCreate()


# Import dataset.
# 'header = True' b/c our file has column headers
# use wasb for HDInsight storage container
upload_startTime = timeit.default_timer()
datalog = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('wasb://assets@datalog2.csv')
upload_stopTime = timeit.default_timer()



# Remaining is a copy of Evan's program
# Remove leading/trailing whitespaces
for col_name in datalog.columns:
    datalog = datalog.withColumn(col_name, trim(col(col_name)))

# Spark interprets the schema of these columns as strings; convert to integers.
datalog = datalog.withColumn("distance", col("distance").cast(IntegerType()))
datalog = datalog.withColumn("amount", col("amount").cast(IntegerType()))


# For viewing a sample of the data
datalog.show()
print("Time to load the dataset into a dataframe:", upload_stopTime - upload_startTime)

# Where we will store the counts per element
counts = spark.createDataFrame([('test',0)],['element','count'])

# Begin.
counting_startTime = timeit.default_timer()

for column in datalog.columns:
    subset = datalog.groupBy(column).count().withColumnRenamed(column, 'element')
    # NOTE:
        # (Distance/Amount)
        # Do we want to count occurences for these columns? 
        # They aren't counted in Stephen's implementation, 
        # so skipping them here for consistency.
    if column != 'distance' and column != 'amount':
        counts = counts.unionByName(subset)

# Condenses duplicates.
counts = counts.groupBy('element').agg(sum('count').alias('count'))

# Finished.
counting_stopTime = timeit.default_timer()
counts.show()
print("Time to count frequencies:", counting_stopTime - counting_startTime)

# Begin.
sorting_startTime = timeit.default_timer()

counts = counts.sort("element", ascending=True)

# Finished.
sorting_stopTime = timeit.default_timer()
counts.show()
print("Time to sort:", sorting_stopTime - sorting_startTime)


# Begin.
amounts_startTime = timeit.default_timer()

totalByCat = datalog.groupBy('transCat').agg(sum('amount').alias('total'))

# Finished.
amounts_stopTime = timeit.default_timer()
totalByCat.show()
print("Time to total the amounts spent by transCat:", amounts_stopTime - amounts_startTime)
