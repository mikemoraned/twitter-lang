package com.houseofmoran.twitter.lang

import java.io.File
import java.io.FileWriter

import ch.hsr.geohash.{BoundingBox, WGS84Point, GeoHash}
import com.fasterxml.jackson.databind.{SerializationFeature, ObjectMapper}
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

    val hashlength = Integer.parseInt(args(0))
    val geojsonFile = new File(args(1))

    val tweetsDf = sqlContext.read.parquet("tweets.consolidated.parquet").cache()

    summarise(tweetsDf)

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

    def wgs84PointToLngLatAlt(in: WGS84Point) = {
      new LngLatAlt(in.getLongitude, in.getLatitude)
    }

    def geoHashBoundingBoxToPolygon(in: BoundingBox) = {
      val lowerRight : LngLatAlt = wgs84PointToLngLatAlt(in.getLowerRight())
      val upperLeft : LngLatAlt = wgs84PointToLngLatAlt(in.getUpperLeft())
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

        val feature = new Feature()
        feature.setId(s)
        feature.setGeometry(geoHashBoundingBoxToPolygon(bb))
        feature
      }
    }.collect()

    val featureCollection = new FeatureCollection
    featureCollection.addAll(asJavaCollection(features))

    val mapper = new ObjectMapper()
    mapper.enable(SerializationFeature.INDENT_OUTPUT)
    mapper.writeValue(new FileWriter(geojsonFile), featureCollection)
  }
}
