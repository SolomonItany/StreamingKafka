package KafkaSpark

import org.apache.spark.sql.{SparkSession, Encoders, DataFrame}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
object Producer {

  def main(args: Array[String]): Unit = {

    // SparkSession configuration
    val spark = SparkSession.builder()
      .appName("Producer")
      .master("local[*]")
      .getOrCreate()

    // Define Kafka producer configuration
    val kafkaServers = "ip-172-31-3-80.eu-west-2.compute.internal:9092" // Replace with your Kafka broker address(es)
    val topicName = "INSURANCE_CLAIM_7" // Your desired Kafka topic name

    // Fetch JSON data from the URL
    val url = "http://18.133.73.36:5003/insurance_claims1"
    val result = Source.fromURL(url).mkString

    // Create a DataFrame from the JSON data
    val jsonData = spark.read.json(spark.createDataset(Seq(result))(Encoders.STRING))

    // Kafka producer configuration
    val props = new java.util.Properties()
    props.put("bootstrap.servers", kafkaServers)
    props.put("key.serializer", "org.apache.spark.common.serialization.StringSerializer") // Adjust serializer if needed
    props.put("value.serializer", "org.apache.spark.common.serialization.StringSerializer") // Adjust serializer if needed

    // Create a Kafka producer
    val producer = new KafkaProducer[String, String](props)

    // Define a Trigger to control data sending frequency
    val trigger = Trigger.ProcessingTime("10 seconds") // Send data every 10 seconds (adjust as needed)

    // Create a streaming query with the trigger
    val query: StreamingQuery = jsonData
      .selectExpr("to_json(struct(*)) AS value") // Convert data to JSON string
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", topicName)
      .trigger(trigger)
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        batch.rdd.foreach { row =>
          val data = row.getString(0) // Assuming data is in the first column

          // Send data to Kafka topic
          val record = new ProducerRecord[String, String](topicName, null, data)
          producer.send(record)
        }
      }
      .start()

    // Wait for the streaming query to terminate (optional)
    query.awaitTermination()

    // Close the Kafka producer
    producer.close()

    println("Streaming data to Kafka topic has started.")
  }
}

