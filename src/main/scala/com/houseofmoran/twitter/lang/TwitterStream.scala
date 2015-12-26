package com.houseofmoran.twitter.lang

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object TwitterStream {

  def fromAuth(ssc: StreamingContext,
               consumerKey : String,
               consumerSecret : String,
               authToken : String,
               authTokenSecret : String) : DStream[Status] = {

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(authToken)
      .setOAuthAccessTokenSecret(authTokenSecret)

    TwitterUtils.createStream(ssc, Some(new OAuthAuthorization(cb.build())))
  }

  def mapToTweetStream(statuses: DStream[Status]) = {
    statuses.map{ status =>
      val location =
        if (status.getGeoLocation() == null)
          null
        else
          new Location(status.getGeoLocation().getLatitude(), status.getGeoLocation().getLongitude)
      val hasMedia = status.getMediaEntities() != null && status.getMediaEntities().length > 0
      new Tweet(status.getUser().getId, status.getId(), status.getText(), location, hasMedia)
    }
  }
}
