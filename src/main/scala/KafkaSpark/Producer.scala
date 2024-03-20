package KafkaSpark

import org.apache.spark.sql.{SparkSession, Encoders}
import scala.io.Source
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
object Producer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Producer").master("local[*]").getOrCreate()
    val url = "http://18.133.73.36:5003/insurance_claims1"
    // Fetch JSON data from the URL
    val result = Source.fromURL(url).mkString

    // Create a DataFrame from the JSON data
    val jsonData = spark.read.json(spark.createDataset(Seq(result))(Encoders.STRING))


    val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
    val topic = "INSURANCE_CLAIM_7"

    // Define a Trigger to control data sending frequency
    val trigger = Trigger.ProcessingTime("3 seconds") // Send data every 10 seconds (adjust as needed)

    // Create a streaming query with the trigger
    val query: StreamingQuery = jsonData
      .selectExpr("to_json(struct(*)) AS value") // Convert data to JSON string
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafkaServer")
      .option("topic", "topic")
      .trigger(trigger)
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        // Process and send data in the batch (similar to the previous example)
      }
      .start()

    // Wait for the streaming query to terminate (optional)
    query.awaitTermination()


    /*val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
    val topicName: String = "InsuranceClaims"

    jsonData.selectExpr("to_json(struct(*)) AS value")
      .selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", topicName).save()

    println("message is loaded to kafka topic")
    Thread.sleep(10000) // wait for 10 seconds before making the next call*/

  }
}