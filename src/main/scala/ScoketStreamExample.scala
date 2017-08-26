import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketStreamExample{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Socket Stream Example")
    conf.setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("file:///home/nadeem/sparkcheckpoint")

    //lines is a DStream sequence of RDDs.
    //new data is automatically made available to this Dstream
    val lines = ssc.socketTextStream(args(0),args(1).toInt)

    //each operation on DStream is applied to each RDD in the DStream
    //The resulting operation is a DStream
    val counts = lines.flatMap(line => line.split(" "))
      .filter(line => line.contains("ERROR"))
      .map(line => (line,1))
      .reduceByKey((acc,value) => acc+value)

    //no need to supply a loop
    counts.print()

    //spark will start reading data
    ssc.start()
    ssc.awaitTermination()

  }
}