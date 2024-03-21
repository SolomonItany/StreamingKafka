package KafkaSpark

import org.apache.spark.sql.{SparkSession, Encoders}
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Producer").master("local[*]").getOrCreate()
    val url = "http://18.133.73.36:5003/insurance_claims1"
    // Fetch JSON data from the URL
    val result = Source.fromURL(url).mkString

    // Create a DataFrame from the JSON data
    val jsonData = spark.read.json(spark.createDataset(Seq(result))(Encoders.STRING))

    jsonData.show(5)


    val jsonStringDF = jsonData.selectExpr("to_json(struct(*)) AS value")

    val kafkaProps = new java.util.Properties()
    kafkaProps.put("bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](kafkaProps)

    val topic = "INSURANCE_CLAIM_12"

    // Assuming jsonStringDF is small enough to collect (be careful with large DataFrames)
    jsonStringDF.collect().foreach { row =>
      val record = new ProducerRecord[String, String](topic, row.getAs[String]("value"))
      producer.send(record)

      // Sleep for 3 seconds between each send
      Thread.sleep(3000)
    }

    producer.close()



  }
}