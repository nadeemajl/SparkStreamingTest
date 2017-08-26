import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketStreamWithGlobalStateUpdate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Socket Stream Global Count Example")
    conf.setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("file:///home/nadeem/sparkcheckpoint")

    //lines is a DStream sequence of RDDs.
    //new data is automatically made available to this Dstream
    val lines = ssc.socketTextStream(args(0),args(1).toInt)

    //each operation on DStream is applied to each RDD in the DStream
    //The resulting operation is a DStream
    val counts = lines.flatMap(line => line.split(" "))
      .map(line => (line,1))//this is a pair RDD
      .updateStateByKey(updateFunction)

    //no need to supply a loop
    counts.print()

    //spark will start reading data
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val currentCount = newValues.sum
    val previousCount = runningCount.getOrElse(0)
    Some(currentCount + previousCount)
  }



}
