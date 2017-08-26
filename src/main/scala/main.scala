package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCounter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Counter")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("file:///home/nadeem/spark-install/spark-2.2.0-bin-hadoop2.7/README.md")
    val tokenizedFileData = textFile.flatMap(line=>line.split(" "))
    val countPrep = tokenizedFileData.map(word=>(word, 1))
    val counts = countPrep.reduceByKey((accumValue, newValue)=>accumValue + newValue)
    val sortedCounts = counts.sortBy(kvPair=>kvPair._2, false)
    sortedCounts.saveAsTextFile("file:///home/nadeem/PluralsightData/ReadMeWordCountViaApp")
  }
}