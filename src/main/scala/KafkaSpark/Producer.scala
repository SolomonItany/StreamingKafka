package KafkaSpark

import org.apache.spark.sql.{SparkSession, Encoders}
import scala.io.Source
import org.apache.spark.sql.streaming.Trigger
object Producer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Producer").master("local[*]").getOrCreate()
    val url = "http://18.133.73.36:5000/Insurance_Claims"
    // Fetch JSON data from the URL
    val result = Source.fromURL(url).mkString

    // Create a DataFrame from the JSON data
    val jsonData = spark.read.json(spark.createDataset(Seq(result))(Encoders.STRING))

    val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
    val topicName: String = "InsuranceClaims"

    val query = jsonData.selectExpr("to_json(struct(*)) AS value")
      .selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", topicName)
      .option("maxFilesPerTrigger", 1)
      .trigger(Trigger.ProcessingTime("30 seconds")) // Add trigger option
      .start()

    query.awaitTermination()
    println("message is loaded to kafka topic")
  }
}

