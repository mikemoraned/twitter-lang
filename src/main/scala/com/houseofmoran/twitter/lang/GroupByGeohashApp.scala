package com.houseofmoran.twitter.lang

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object GroupByGeohashApp {

  def summarise(df: DataFrame) = {
    df.printSchema()
    df.show()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByGeohashApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val tweetsDf = sqlContext.read.parquet("tweets.consolidated.parquet").cache()

    summarise(tweetsDf)

    val hashlength = 4
    val toGeoHashString : (Row => String) = {
      case Row(latitude : Double, longitude : Double) =>
        GeoHash.withCharacterPrecision(latitude, longitude, hashlength).toBase32
    }

    val withGeoHashDF = tweetsDf.withColumn("geohash", callUDF(toGeoHashString, StringType, col("location")))

    summarise(withGeoHashDF)

    withGeoHashDF.registerTempTable("tweets")
    val byFreq = sqlContext.sql(
      """
            select geohash,
                   count(*) as c,
                   100 * count(*) / max(t.c) as p
            from tweets,
                 (select count(*) as c from tweets) t
            group by geohash
            order by p desc
      """.stripMargin)

    summarise(byFreq)
  }
}
