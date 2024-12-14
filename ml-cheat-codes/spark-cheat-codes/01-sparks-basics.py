
# Importing SparkSession from pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField,StructType, DoubleType

# Creating a SparkSession
spark = SparkSession.builder.appName('myapp').getOrCreate()
# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

path = "/home/raushan/data-science/ml-cheat-codes/sample-data/"

file_name = path + "sample_superstore.csv"

df = spark.read.option('header', True).csv(file_name)

# Displaying the DataFrame
display(df)
display(df.show())


df.head(10)
df.show(n=2)

df.columns
df.printSchema()

df.limit(5).show()

schema = StructType([
StructField("Sales", DoubleType(),True)
])

df = spark.read.option('header','True').option('delimiter',',').schema(schema).csv(file_name)



# Define the schema with 'Sales' as DoubleType and others as StringType
dtschema = StructType([
    StructField("Sales", DoubleType(), True)
])



df = spark.read.option('header', 'True').option('delimiter', ',').schema(dtschema).csv(file_name)

temp_df.schema
temp_df.printSchema()

# Define the schema with 'Sales' as DoubleType and others as StringType
schema = StructType([StructField("Sales", DoubleType(), True)])
for col in temp_df.columns:
    if col != "Sales":
        schema.add(StructField(col, StringType(), True))

df = spark.read.option('header', 'True').option('delimiter', ',').schema(schema).csv(file_name)
df.printSchema()



display(df['Ship Mode', 'Segment'].show())

display(df[df.columns].show(5))


df.limit(6).toPandas()

from pyspark.sql.types import DoubleType
df1 = df.withColumn("Postal Code", df["Postal Code"].cast(DoubleType()))

df1.dtypes

df.groupBy(['Country','Segment']).agg({'Sales':'sum'}).show()

df.filter(df['Ship Mode'] == 'Standard Class').groupBy('Segment').avg('Sales').show()

display(df
.filter(((df.Origin != 'SAN') & (df.DayOfWeek < 3)) | (df.Origin == 'SFO'))
.groupBy('DayOfWeek')
.avg('arrdelaydouble'))

display(df
.filter(df.Origin != 'SAN')
.filter(df.DayOfWeek < 3)
.groupBy('DayOfWeek')
.avg('arrdelaydouble'))

display(df
.filter(df.Origin == 'SAN')
.groupBy('DayOfWeek')
.avg('arrdelaydouble')
.sort('DayOfWeek'))

display(df
.filter((df.Origin == 'SAN') & (df.Dest == 'SFO'))
.groupBy('DayOfWeek')
.avg('arrdelaydouble'))

display(df
.filter(((df.Origin != 'SAN') & (df.DayOfWeek < 3)) | (df.Origin == 'SFO'))
.groupBy('DayOfWeek')
.avg('arrdelaydouble'))

from pyspark.sql.functions import mean, round
df.filter(df['Ship Mode'] == 'Standard Class').groupBy('Segment').avg('Sales').alias('Avgsales').show()


df.filter(df['Ship Mode'] == 'Standard Class').groupBy('Segment').agg(round(mean('Sales'),2).alias('AvgArrDelay')).show()

display(df
.filter(df.Origin == 'SAN')
.groupBy('DayOfWeek')
.avg('arrdelaydouble')
.sort('DayOfWeek'))

display(df.filter(df.Origin == 'SAN')
.groupBy('DayOfWeek')
.avg('arrdelaydouble')
.orderBy('DayOfWeek')

display(df
.filter(df.Origin == 'SAN')
.groupBy('DayOfWeek')
.agg(round(mean('arrdelaydouble'),2).alias('AvgArrDelay'))
.sort(desc('AvgArrDelay')))

from pyspark.sql.functions import min, max
display(df
.filter(df.Origin == 'SAN')
.groupBy('DayOfWeek')
.agg(min('arrdelaydouble').alias('MinDelay')
, max('arrdelaydouble').alias('MaxDelay')
, (max('arrdelaydouble')-min('arrdelaydouble')).alias('Spread'))
)

display(df
.filter(df.Origin.isin(['SFO','SAN','OAK']))
.filter(df
.DayDate.between(start_date,end_date)
)
.groupBy('Origin','DayDate')
.agg(mean('ArrDelay'))
.orderBy('DayDate'))

airport_list = [row.Origin for row in df.select('Origin').distinct().limit(5).collect()]

from pyspark.sql.functions import when
df = df.withColumn('State',
when(col('Origin') == 'SAN', 'California')
.when(df.Origin == 'LAX', 'California')
.when(df.Origin == 'SAN', 'California')
.when((df.Origin == 'JFK') | (df.Origin == 'LGA') |
(df.Origin == 'BUF'), 'New York')
.otherwise('Other')
)

def bins(flights):
    if flights < 400:
        return 'Small'
    elif flights >= 1000:
        return 'Large'
    else:
        return 'Medium'


from pyspark.sql.functions import count, mean

df_temp = df.groupBy('Origin','Dest').agg(
count('Origin').alias('count'), mean('arrdelaydouble').alias('AvgDelay'))

from pyspark.sql.types import StringType

bins_udf = udf(bins, StringType())
df_temp = df_temp.withColumn("Size", bins_udf("count"))


display(df1.filter(df.Discount.isNull()))

from pyspark.sql.functions import count, when, isnull

display(df1.select([count(when(isnull(c), c)).alias(c) for c in df.columns]))

df1.count(), len(df1.columns)

df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).limit(7).toPandas()

df.select([count(when(isnull('Segment'), 'Segment'))]).limit(7).toPandas()

from pyspark.sql.functions import col
cols = [c for c in df.columns if df.filter(col(c).isNull()).count() > 0 ]


from pyspark.ml.feature import Imputer
df.withColumn('Sales', df.Sales.cast('double'))

