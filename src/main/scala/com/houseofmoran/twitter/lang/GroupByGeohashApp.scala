package com.houseofmoran.twitter.lang

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object GroupByGeohashApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByGeohashApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val tweetsDf = sqlContext.read.parquet("tweets.consolidated.parquet").cache()

    tweetsDf.printSchema()
    tweetsDf.show()

    val toGeoHashString : (Row => String) = {
      case Row(latitude : Double, longitude : Double) =>
        GeoHash.withCharacterPrecision(latitude, longitude, 4).toBase32
    }

    val withGeoHashDF = tweetsDf.withColumn("geohash", callUDF(toGeoHashString, StringType, col("location")))

    withGeoHashDF.printSchema()
    withGeoHashDF.show()
  }
}
