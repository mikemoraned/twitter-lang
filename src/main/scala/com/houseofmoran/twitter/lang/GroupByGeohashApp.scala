package com.houseofmoran.twitter.lang

import ch.hsr.geohash.{BoundingBox, WGS84Point, GeoHash}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.{SparkConf, SparkContext}
import org.geojson._

import scala.collection.JavaConversions.asJavaCollection

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
      .cache()

    summarise(byFreq)

    implicit def wgs84PointToLngLatAlt(in: WGS84Point) = {
      new LngLatAlt(in.getLongitude, in.getLatitude)
    }

    implicit def geoHashBoundingBoxToPolygon(in: BoundingBox) = {
      val lowerRight : LngLatAlt = in.getLowerRight()
      val upperLeft : LngLatAlt = in.getUpperLeft()
      val polygon = new Polygon(
        upperLeft,
        new LngLatAlt(lowerRight.getLongitude, upperLeft.getLatitude),
        lowerRight,
        new LngLatAlt(upperLeft.getLongitude, lowerRight.getLatitude),
        upperLeft
      )
      polygon
    }

    val features = byFreq.select("geohash").map {
      case Row(s: String) => {
        val bb = GeoHash.fromGeohashString(s).getBoundingBox()
//        val centre = bb.getCenterPoint

        val feature = new Feature()
        feature.setId(s)
//        val geometry = new Point(new LngLatAlt(centre.getLongitude, centre.getLatitude))
        val geometry : Polygon = bb

        feature.setGeometry(geometry)
        feature
      }
    }.collect()

    val featureCollection = new FeatureCollection
    featureCollection.addAll(asJavaCollection(features))

    val s = new ObjectMapper().writeValueAsString(featureCollection)
    println(s)
  }
}
