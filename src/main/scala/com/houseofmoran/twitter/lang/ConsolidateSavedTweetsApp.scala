package com.houseofmoran.twitter.lang

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ConsolidateSavedTweetsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SummariseSavedTweetsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val tweetsFile = sqlContext.read.parquet("tweets/*.parquet")
    val tweetsDF = tweetsFile.coalesce(100).toDF()

    println(s"Count of tweets: ${tweetsDF.count()}")
    println("schema:");
    println(tweetsDF.schema)

    tweetsDF.write.mode(SaveMode.Overwrite).format("parquet").save("tweets.consolidated.parquet")
  }
}
