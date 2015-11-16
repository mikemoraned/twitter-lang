package com.houseofmoran.twitter.lang

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SummariseSavedTweetsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SummariseSavedTweetsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val tweetsFile = sqlContext.read.parquet("tweets.parquet")
    val tweetsDF = tweetsFile.toDF()

    println(s"Count of tweets: ${tweetsDF.count()}")
    tweetsDF.show(false)
  }
}