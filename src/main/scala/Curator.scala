import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object Curator extends App {

  val kafkaParams = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "bix",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
  val topic = "enriched"
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Sample Spark SQL")
    .master("local[*]")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))
  val ssDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferschema", "true")
    .load("hdfs://quickstart.cloudera/user/fall2019/dhrumil/sprint3/EnrichedFile/enricher.csv")
  ssDF.createOrReplaceTempView("b")
  ssDF.show(1)
  val x: DStream[String] = stream.map(record => record.value())
  x.foreachRDD(microRdd => {
    import spark.implicits._
    val df = microRdd
      .map(_.split(",", -1))
      .map(x => Enrichedtrip(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12),
        x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22)))
      .toDF
    df.show(3)
    df.createOrReplaceTempView("a")
    val enrichedbixi = ssDF.join(df, ssDF.col("stationId") ===
      df.col("stationId"), "left")
//    enrichedbixi.coalesce(1).write.mode("append").option("header", "true").csv("hdfs://quickstart.cloudera/user/fall2019/dhrumil/sprint3/result")
  })
  ssc.start()
  ssc.awaitTermination()
  ssc.stop(true)
}
