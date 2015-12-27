package com.houseofmoran.twitter.lang.hasmedia

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object HasMedia {

  val boolToDouble : (Boolean => Double) = (b : Boolean) => if (b) 1.0 else 0.0

  def normalise(df: DataFrame) : DataFrame = {
    val normalised = df.withColumn("label", callUDF(boolToDouble, DoubleType, col("hasMedia")))

    normalised.printSchema()
    normalised.show()

    normalised
  }
}
