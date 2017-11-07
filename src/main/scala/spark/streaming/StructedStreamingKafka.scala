package spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * Created by 71065 on 2017/11/7 0007.
  */
object StructedStreamingKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("StructedStreamingKafka")
      .getOrCreate()
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.245.216:9092")
      .option("subscribe", "deviceLog")
      .load()
    val stream = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .format("console")
      .trigger(ProcessingTime(2000L))
      .start()
    stream.awaitTermination()

  }
}
