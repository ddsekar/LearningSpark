package com.dhan.learnspark

import org.apache.spark.{SparkConf, SparkContext}

object Wordcount {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val outputFile = args(1)

    val conf = new SparkConf().setMaster("local").setAppName("My App");
    val sc = new SparkContext(conf);

    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.saveAsTextFile(outputFile)
//    words.foreach(println)
  }
}
