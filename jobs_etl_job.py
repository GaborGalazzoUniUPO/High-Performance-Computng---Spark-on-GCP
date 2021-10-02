import sys
from datetime import datetime, timedelta
import os
from pyspark.sql import functions as F

# Setup time variables
from pyspark.sql import SparkSession

bucket_name = sys.argv[1]
if len(sys.argv) > 2:
    minus = int(sys.argv[2])
else:
    minus = 0

today = (datetime.today() - timedelta(days=minus)).strftime("%Y%m%d")

# Retrieve spark session
spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()

# Extraction phase
source_df = spark.read \
    .option("header", True) \
    .csv("gs://%s/dpc-covid19-ita-province-%s.csv" % (bucket_name, today))

# Transformation phase
small_source_df = source_df.select(
    "stato",
    "codice_regione",
    "denominazione_regione",
    "codice_provincia",
    "denominazione_provincia",
    "totale_casi",
    F.lit(today).alias("data"))

# Loading phase
small_source_df.write \
    .partitionBy("data", "stato", "codice_regione", "codice_provincia") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .parquet("gs://%s/dpc-covid19-ita-province" % bucket_name)

# cleanup
os.popen('gsutil rm gs://%s/dpc-covid19-ita-province-%s.csv' % (bucket_name, today)).read()
