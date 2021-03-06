enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("com.houseofmoran.twitter.lang.ReadTweetsApp")

fork in run := true

javaOptions in run += "-Xms2G"
javaOptions in run += "-Xmx4G"

name := """twitter-lang"""

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-sql" % "1.5.0",
  "org.apache.spark" %% "spark-mllib" % "1.5.0",
  "org.apache.spark" %% "spark-streaming" % "1.5.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.5.0")

libraryDependencies ++= Seq(
  "ch.hsr" % "geohash" % "1.1.0",
  "de.grundid.opendatalab" % "geojson-jackson" % "1.5.1" intransitive()
)

