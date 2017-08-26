import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SummaryAndInverseFunction {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Summary and inverse function example")
    conf.setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("file:///home/nadeem/sparkcheckpoint")

    val lines = ssc.socketTextStream(args(0),args(1).toInt).map(_.toInt)

    val counts = lines.reduceByWindow(
                          {(x:Int ,y:Int) => x+ y},
                          {(x:Int,y:Int) => x-y},
                          Seconds(9),
                          Seconds(3))

    counts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
