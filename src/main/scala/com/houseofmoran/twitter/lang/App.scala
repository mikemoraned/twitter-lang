package com.houseofmoran.twitter.lang

import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.sql.SQLContext
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization    
    
object App {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TwitterLangApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val windowLength = Seconds(10)
    val ssc = new StreamingContext(sc, windowLength)
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

    geoStatuses.foreachRDD { statuses =>
			     statuses.foreach{ status => println(status) }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
