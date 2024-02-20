import pyspark
from delta import *
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import * 
import json 
from delta import configure_spark_with_delta_pip


data = []
with open("news_data.json", "r") as f: 
    received = json.load(f)
    for line in received: 
        url, title, content, date = line['url'], line['title'], line['content'], line['date']
        data.append((url, title, content,date))

builder = pyspark.sql.SparkSession.builder.master("local").appName("Testing-Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.ui.port", "4050") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



def create_table(): 
    data = []
    with open("news_data.json", "r") as f: 
        received = json.load(f)
        for line in received: 
            url, title, content, date = line['url'], line['title'], line['content'], line['date']
            data.append((url, title, content,date))
    print("Creating Delta table..")
    schema = StructType(
        [
            StructField("url", StringType(), True), 
            StructField("title", StringType(), True), 
            StructField("content", StringType(), True), 
            StructField("date", StringType(), True), 
        ]
    )
    sample_df = spark.createDataFrame(data=data, schema=schema)
    sample_df.write.mode(saveMode = "overwrite").format("delta").save("saved/delta-tables")

def read_table(): 
    print("Reading delta file...")
    dfs = spark.read.format("delta").load("saved/delta-tables")
    dfs.show()

def update_table(): 
    print("Updating data...")
    table = DeltaTable.forPath(spark, "saved/delta-tables")
    table.toDF().show()
    table.update(
        condition= expr("date == '2024-02-15 23:53:18'"), 
        set = {
            "url": lit("https://hello.com"), "title": lit("This is testing"), 
            "content": lit("Testing content"), "date": lit("Hello")
        }
    )
    table.toDF().show()
def upsert_table(): 
    print("Upserting data...")
    table = DeltaTable.forPath(spark, "saved/delta-tables")
    table.toDF().show()
    data = [("Site", "Titling", "Content", "Datet")]
    schema = StructType(
        [
            StructField("url", StringType(), True), 
            StructField("title", StringType(), True), 
            StructField("content", StringType(), True), 
            StructField("date", StringType(), True), 
        ]
    )
    newData = spark.createDataFrame(data=data, schema=schema)
    table.alias("oldData") \
    .merge(
    newData.alias("newData"),
    "oldData.date = newData.date") \
    .whenMatchedUpdate(
    set={"url": col("newData.url"), "title": col("newData.title"), "content": col("newData.content"),
         "date": col("newData.date")}) \
    .whenNotMatchedInsert(
    values={"url": col("newData.url"), "title": col("newData.title"), "content": col("newData.content"),
            "date": col("newData.date")}) \
    .execute()

def delete_table(): 
    table = DeltaTable.forPath(spark, "saved/delta-tables")
    table.delete(condition=expr("date == 'Hello'"))
    table.toDF().show()

def view_history(): 
    print("Read old data...")
    df_version0 = spark.read.format("delta").option("versionAsOf",0).load("saved/delta-tables")
    df_version0.show()
    df_versionzone = spark.read.format("delta").option("versionAsOf",1).load("saved/delta-tables")
    df_versionzone.show()

create_table()
read_table()
#update_table()
upsert_table()
view_history()