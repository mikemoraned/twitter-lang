package com.houseofmoran.twitter.lang

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
    
object ReadTweetsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadTweetsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val batchInterval = Minutes(1)
    val ssc = new StreamingContext(sc, batchInterval)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val twitterStream = TwitterStream.fromAuth(ssc, args(0), args(1), args(2), args(3))

    val geoStatuses = twitterStream.
      filter(status => status.getGeoLocation() != null)

    val tweetStream = TwitterStream.mapToTweetStream(geoStatuses)

    val windowSize = batchInterval * 5
    tweetStream.window(windowSize, windowSize).foreachRDD( (tweetsRDD, time) => {
      val tweetsDF = tweetsRDD.toDF()
      tweetsDF.show()
      tweetsDF.write.
        format("parquet").
        save(s"tweetsN/${time.milliseconds}.parquet")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
