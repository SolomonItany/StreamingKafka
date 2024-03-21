package KafkaSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Consumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Consumer").master("local[*]").getOrCreate()


    // Define the Kafka topic to subscribe to
    val topic = "INSURANCE_CLAIM_13"

    //Define the Kafka parameters
    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "failOnDataLoss" -> "false",
      "startingOffsets" -> "earliest"
    )

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("AGE", StringType, nullable = true),
      StructField("BIRTH", StringType, nullable = true),
      StructField("BLUEBOOK", StringType, nullable = true),
      StructField("CAR_AGE", StringType, nullable = true),
      StructField("CAR_TYPE", StringType, nullable = true),
      StructField("CAR_USE", StringType, nullable = true),
      StructField("CLAIM_FLAG", StringType, nullable = true),
      StructField("CLM_AMT", StringType, nullable = true),
      StructField("CLM_FREQ", StringType, nullable = true),
      StructField("EDUCATION", StringType, nullable = true),
      StructField("GENDER", StringType, nullable = true),
      StructField("HOMEKIDS", StringType, nullable = true),
      StructField("HOME_VAL", StringType, nullable = true),
      StructField("ID", StringType, nullable = true),
      StructField("INCOME", StringType, nullable = true),
      StructField("KIDSDRIV", StringType, nullable = true),
      StructField("MSTATUS", StringType, nullable = true),
      StructField("MVR_PTS", StringType, nullable = true),
      StructField("OCCUPATION", StringType, nullable = true),
      StructField("OLDCLAIM", StringType, nullable = true),
      StructField("PARENT1", StringType, nullable = true),
      StructField("RED_CAR", StringType, nullable = true),
      StructField("REVOKED", StringType, nullable = true),
      StructField("TIF", StringType, nullable = true),
      StructField("TRAVTIME", StringType, nullable = true),
      StructField("URBANICITY", StringType, nullable = true),
      StructField("YOJ", StringType, nullable = true)
    ))

    // Read the JSON messages from Kafka as a DataFrame and write to hdfs
    val df = spark.readStream.format("kafka").options(kafkaParams).option("subscribe", topic)
    .load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")

    // Write  DataFrame as CSV files to HDFS



    df.writeStream.format("csv").option("checkpointLocation", "/tmp/USUK30/Solomon/Kafka/checkpoint").option("path", "/tmp/USUK30/Solomon/Kafka/Insurance_claims").start().awaitTermination()

  }

}