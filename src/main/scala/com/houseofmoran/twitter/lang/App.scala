package com.houseofmoran.twitter.lang

import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.sql.SQLContext
    
object App {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TwitterLangApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val windowLength = Seconds(10)
    val ssc = new StreamingContext(sc, windowLength)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val twitterStream = TwitterUtils.createStream(ssc, None)

    val geoStatuses = twitterStream.
      filter(status => status.getGeoLocation() != null)

    geoStatuses.foreachRDD { statuses =>
			     statuses.foreach{ status => println(status) }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
