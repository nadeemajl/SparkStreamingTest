import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReduceByKeyAndWindowExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Summary and inverse function example")
    conf.setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("file:///home/nadeem/sparkcheckpoint")

    val lines = ssc.socketTextStream(args(0),args(1).toInt)

    val counts = lines.flatMap(line => line.split(" "))
        .filter(word => word.contains("ERROR"))
      .map(word => (word,1))
      .reduceByKeyAndWindow(_+_,_-_,Seconds(15),Seconds(6))

    counts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
