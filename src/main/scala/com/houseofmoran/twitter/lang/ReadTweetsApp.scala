package com.houseofmoran.twitter.lang

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
    
object ReadTweetsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadTweetsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val batchInterval = Minutes(1)
    val ssc = new StreamingContext(sc, batchInterval)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(args(0))
      .setOAuthConsumerSecret(args(1))
      .setOAuthAccessToken(args(2))
      .setOAuthAccessTokenSecret(args(3))
    
    val twitterStream = TwitterUtils.createStream(ssc, Some(new OAuthAuthorization(cb.build())))

    val geoStatuses = twitterStream.
      filter(status => status.getGeoLocation() != null)

    val tweetStream = geoStatuses.map{ status =>
      val location = new Location(status.getGeoLocation().getLatitude(), status.getGeoLocation().getLongitude)
      val hasMedia = status.getMediaEntities() != null && status.getMediaEntities().length > 0
      new Tweet(status.getUser().getId, status.getId(), status.getText(), location, hasMedia)
    }

    val windowSize = batchInterval * 1
    tweetStream.window(windowSize, windowSize).foreachRDD( (tweetsRDD, time) => {
      val tweetsDF = tweetsRDD.toDF()
      tweetsDF.show()
      tweetsDF.write.
        format("parquet").
        save(s"tweets/${time.milliseconds}.parquet")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
