package com.streaming.dataframe

import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.kafka010.KafkaSourceProvider
import org.apache.kafka.clients._

object create_df_from_kafka_topic {

  def main(args: Array[String]):Unit = {
    println("Apache Spark Application Started.....")

    val spark = SparkSession.builder()
      .appName("Create DataFame from Kafka Topic")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val kafka_topic_name = "transactions"
    val kafka_bootstrap_servers = "localhost:9092"

    val kafka_df = spark.read
      .format(source = "kafka")
      .option("kafka.bootstrap.servers","kafka_bootstrap_servers")
      .option("subscribe","kafka_topic_name")
      .load()

    kafka_df.printSchema()

    val kafka_df1 = kafka_df.selectExpr(exprs = "CAST(value as STRING)")

    val kafka_df_schema = StructType(Array(
      StructField("accountNumber",IntegerType,true),
      StructField("description",StringType,true),
      StructField("currentDate",DateType,true),
      StructField("txAmount",DoubleType,true)
    ))

    val kafka_df2 = kafka_df1.select(from_json(col("value"),kafka_df_schema)
        .as("Kafka_Stream_Details"))

    val kafka_df3 = kafka_df2.select("Kafka_Stream_Details.*")

    kafka_df3.printSchema()
    kafka_df3.show(10,truncate = false)

  }

}
