# Real-time streaming - Kafka Producer

## 1. Introduction
Real-time streaming data processing encompasses the continuous execution of data processing tasks on live data streams. In this guide, 
we'll detail the process of streaming data ingestion, transformation, and loading into a target system (such as Apache Kafka). Specifically,
we'll focus on setting up a Kafka producer using Scala to publish real-time data streams. Additionally, we'll highlight the pivotal role 
of version control using GitHub for code hosting and Jenkins for continuous integration and continuous deployment (CI/CD) orchestration,
ensuring the seamless development and deployment of our real-time streaming pipeline

## Important steps to establish a kafka producer in scala
1. Importing necessary modules.
2. Configuring the maven dependencies.
3. Establishing spark context 
4. Parsing data from a streaming source. Note in our case we parse data from a static Api source
5. set the kafka parameters
6. Initialize Kafka producer
7. create a topic
8. simulating a stream from API to kafaka.

## Producer code overview
```scala
package KafkaSpark

import org.apache.spark.sql.{SparkSession, Encoders}
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder().appName("Producer").master("local[*]").getOrCreate()

    // URL of the API endpoint to fetch JSON data
    val url = "http://18.133.73.36:5003/insurance_claims1"

    // Fetch JSON data from the URL
    val result = Source.fromURL(url).mkString

    // Create a DataFrame from the JSON data
    val jsonData = spark.read.json(spark.createDataset(Seq(result))(Encoders.STRING))

    // Display the first 5 rows of the DataFrame
    jsonData.show(5)

    // Convert DataFrame to JSON string
    val jsonStringDF = jsonData.selectExpr("to_json(struct(*)) AS value")

    // Kafka producer configuration
    val kafkaProps = new java.util.Properties()
    kafkaProps.put("bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Initialize Kafka producer
    val producer = new KafkaProducer[String, String](kafkaProps)

    // Kafka topic
    val topic = "INSURANCE_CLAIM_13"

    // Send each JSON record to Kafka topic
    jsonStringDF.collect().foreach { row =>
      val record = new ProducerRecord[String, String](topic, row.getAs[String]("value"))
      producer.send(record)

      // Sleep for 3 seconds between each send
      Thread.sleep(3000)
    }

    // Close the Kafka producer
    producer.close()
  }
}
```
## 2. The provided scala code executes the following steps:

- **Importing libraries**
    - `package KafkaSpark`: defines the package name where the Scala code resides. It helps organize code
      into logical units and avoids naming conflicts.
    - `import org.apache.spark.sql.{SparkSession, Encoders}`: SparkSession is the entry point to Spark SQL
      functionality, and Encoders are used for converting Scala objects to and from the internal Spark SQL representation
    - `Import scala.io.Source`: The scala.io package is used for reading text files from various sources, including URLs and files
    - `Import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}`: This line imports classes KafkaProducer
      and ProducerRecord from the org.apache.kafka.clients.producer package. These classes are used for producing records
      to a Kafka topic. KafkaProducer is used to send messages to Kafka topics, while ProducerRecord represents a single
      message that can be sent to a Kafka topic
- **Object Definition (object Producer)**
     - AN object is a singleton instance of a class. It is used here to define a standalone object named Producer.
       The Producer object encapsulates the functionality for producing data to Kafka
- **Main Method (def main(args: Array[String]): Unit = { ... })**
     - This is the entry point of the program. It defines the main method, which is executed when the program is run.
       Inside the main method, the code for producing data to Kafka is implemented.
- **Initializing SparkSession**
     - SparkSession.builder().appName("Producer").master("local[*]").getOrCreate(): This initializes a SparkSession, which is the
       entry point to Spark SQL. The appName method sets the name of the Spark application to "Producer".The master method specifies 
       the Spark master URL. In this case, it runs Spark locally using all available cores (local[*]).The getOrCreate method creates a 
       new SparkSession or gets an existing one if available.
- **Fetching JSON Data from URL**
     - Source.fromURL(url).mkString fetches JSON data from the specified URL and stores it as a string in the result variable
- **Create a DataFrame from JSON Data:**
     - Using `spark.read.json`, JSON data fetched from the URL is converted into a DataFrame called `jsonData`.
       The data is read as a Dataset of Strings and then inferred into a DataFrame.
- **Convert DataFrame to JSON String:**
     - `jsonData.selectExpr("to_json(struct(*)) AS value")` converts the DataFrame `jsonData` into a DataFrame of JSON strings named `jsonStringDF`.
- **Kafka Producer Configuration:**
     - Properties for Kafka connection are defined using a `java.util.Properties` It specifies the Kafka brokers' addresses and serializers
       for the key and value.
- **Initialize Kafka Producer:**
     - A KafkaProducer object is created using the specified properties.
- **Kafka Topic:**
     - The Kafka topic to which records will be sent is defined as `INSURANCE_CLAIM_13`.
- **Sending Data to Kafka:**
     - Each JSON record from `jsonStringDF` is sent to the Kafka topic using a `KafkaProducer`. A ProducerRecord is created for each record,
     and the record is sent to Kafka using the `producer.send()` method.
- **Delay Between Sends:**
    - A 3-second delay is introduced between sending each record to Kafka using `Thread.sleep(3000)`.
- **Close the Kafka Producer:** Finally, the Kafka producer is closed using the `producer.close()` method to release resources.

## 3. GitHub and Jenkins Integration
Link to Jenkins Full Load Scala Job: [Jenkins Scala Job](http://3.9.191.104:8080/job/Solomon's%20Jobs/job/KafkaStreamingConsumer/)

GitHub serves as the repository for hosting the Scala code, enabling version control, collaboration, and code sharing. Jenkins plays
a pivotal role in our Continuous Integration/Continuous Deployment (CI/CD) workflow. It continuously monitors GitHub repositories 
for changes and automatically triggers builds and tests whenever new code is pushed. Whenever there's a new commit or code change 
in the GitHub repository, Jenkins executes the defined Scala job, ensuring seamless integration and deployment of our Spark applications.

## 4. Explanation:
The code reads data from a from an API, creates a kafka topic and sends meassages to that topic. GitHub ensures
version control and collaborative development of the scala code. Jenkins automates the execution of the PySpark job as part of 
the CI/CD pipeline, ensuring seamless deployment and data processing
      
  
