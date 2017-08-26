import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SlidingWindowExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sliding Window Example")
    conf.setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("file:///home/nadeem/sparkcheckpoint")

    val lines = ssc.socketTextStream(args(0),args(1).toInt)
    val counts = lines.countByWindow(Seconds(9),Seconds(3))

    counts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
