package KafkaSpark

import org.apache.spark.sql.{SparkSession, Encoders}
import scala.io.Source

object Producer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Producer")
      .master("local[*]")
      .getOrCreate()

    val url = "https://api.tfl.gov.uk/Line/victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"

    // Fetch JSON data from the URL
    val result = Source.fromURL(url).mkString

    // Create a DataFrame from the JSON data
    val jsonData = spark.read.json(spark.createDataset(Seq(result))(Encoders.STRING))
    jsonData.show()
  }
}

