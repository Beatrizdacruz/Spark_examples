from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit

def startSession():

    spark = SparkSession.builder\
            .master("local[*]")\
            .appName('Estudo_PySpark')\
            .getOrCreate()
    csv_file = 'data/Top_1000_Companies_Dataset.csv'
    #df = spark.read.csv(csv_file)
    df = spark.read.format('csv').options(header='true', inferSchema='true', delimiter = ";").load(csv_file)

    df.printSchema()
    df.columns


    data_schema = [
                StructField('company_name', StringType(), True),
                StructField('url', StringType(), True),
                StructField('city', StringType(), True),
                StructField('state', StringType(), True),
                StructField('country', StringType(), True),
                StructField('employees', IntegerType(), True),
                StructField('linkedin_url', StringType(), True),
                StructField('founded', IntegerType(), True),
                StructField('Industry', StringType(), True),
                StructField('GrowjoRanking', IntegerType(), True),
                StructField('PreviousRanking', IntegerType(), True),
                StructField('estimated_revenues', IntegerType(), True),
                StructField('job_openings', IntegerType(), True),
                StructField('keywords', StringType(), True),
                StructField('LeadInvestors', StringType(), True),
                StructField('Accelerator', StringType(), True),
                StructField('btype', StringType(), True),
                StructField('valuation', StringType(), True),
                StructField('total_funding', StringType(), True),
                StructField('product_url', StringType(), True),
                StructField('indeed_url', StringType(), True),
                StructField('growth_percentage', StringType(), True),
                StructField('contact_info', StringType(), True),
                ]

    final_struc = StructType(fields = data_schema)

    data = spark.read.csv('data/Top_1000_Companies_Dataset.csv',sep = ',', header = True, schema = final_struc)

    #data.dtypes
    #data.describe().show(5) #mostra as estatísticas
    data.show(5)
    return data

def renameAndUpdateColumn(data):
    data = data.withColumnRenamed('url', 'domain')
    data = data.drop('contact_info')
    data.show()

    return

def selectCompany(data):

    data.filter(col('founded') >= lit('2020')).show()