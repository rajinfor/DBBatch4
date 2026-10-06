from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, col

@dp.table(name="circuits_dlt")
def bronze_circuits():
    
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles","true")
        .option("header","true")
        .option("inferschema","true")
        .load("/Volumes/batch4/class/inputfiles/batch4_files")
        .withColumn("LoadFile", col("_metadata.file_name"))
        .withColumn("DateLoaded", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))                
    )