import sys
from datetime import datetime, timedelta
import os
from pyspark.sql import functions as F
import json
from pyspark.sql import SparkSession

# Setup time variables

bucket_name = sys.argv[1]
if len(sys.argv) > 2:
    minus = int(sys.argv[2])
else:
    minus = 0

today = (datetime.today() - timedelta(days=minus)).strftime("%Y%m%d")
yesterday = (datetime.today() - timedelta(days=minus + 1)).strftime("%Y%m%d")

# Retrieve spark session
spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()

# load data from DW
small_source_df = spark.read.parquet("gs://%s/dpc-covid19-ita-province" % bucket_name).where(F.col("data") == today)

# calculate aggregate values
df_by_region = small_source_df.groupBy("codice_regione").agg(
    F.first(F.col("denominazione_regione")).alias("denominazione_regione"),
    F.first(F.col("data")).alias("date"),
    F.sum(F.col("totale_casi")).alias("totale_casi")) \
    .select("denominazione_regione", "totale_casi", "date")

# compute deltas with old data if any
sc = spark.sparkContext
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jvm.java.net.URI.create("gs://%s" % bucket_name),
    sc._jsc.hadoopConfiguration(),
)
if fs.exists(sc._jvm.org.apache.hadoop.fs.Path("dpc-covid19-ita-region")):
    yesterday_data = spark.read.parquet("gs://%s/dpc-covid19-ita-region" % bucket_name).where(F.col("date") == yesterday)
    yesterday_data.show()
    joined = df_by_region.join(yesterday_data,
                               yesterday_data.denominazione_regione == df_by_region.denominazione_regione, how='left')
    region_recap_df = joined.withColumn("delta",
                                        df_by_region.totale_casi - F.when(yesterday_data.totale_casi.isNull(),
                                                                          0).otherwise(yesterday_data.totale_casi)) \
        .select(df_by_region.denominazione_regione, df_by_region.totale_casi, "delta", df_by_region.date)
    pass
else:
    region_recap_df = df_by_region.withColumn("delta", F.lit(0))
region_recap_df = region_recap_df.orderBy(F.desc("delta"), F.desc("totale_casi"))

# materialize view for next time
region_recap_df.write \
    .partitionBy("date") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .parquet("gs://%s/dpc-covid19-ita-region" % bucket_name)

# generate recap file
recap = region_recap_df.collect()
json_recap = []
for row in recap:
    json_recap.append(row.asDict())
with open('data.json', 'w') as outfile:
    json.dump(json_recap, outfile)
    outfile.close()
    os.popen('gsutil mv data.json gs://%s/dpc-covid19-ita-region-%s.json' % (bucket_name, today)).read()
