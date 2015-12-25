package com.houseofmoran.twitter.lang

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SummariseSavedTweetsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SummariseSavedTweetsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val tweetsFile = sqlContext.read.parquet("tweetsN/*.parquet")
    val tweetsDF = tweetsFile.coalesce(100).toDF()

    println(s"Count of tweets: ${tweetsDF.count()}")
    tweetsDF.sample(false, 0.1).show(false)
  }
}
