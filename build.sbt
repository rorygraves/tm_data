scalaVersion := "2.13.13"

name := "tm_data"
organization := "tm_data"
version := "1.0"

scalacOptions += "-deprecation"

libraryDependencies ++= List(
  "com.lihaoyi" %% "requests" % "0.9.0",
  "com.lihaoyi" %% "pprint" % "0.9.0",
  "com.lihaoyi" %% "ujson" % "4.0.0",
  "org.apache.commons" % "commons-csv" % "1.12.0",
  "com.typesafe.slick" %% "slick" % "3.5.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.5.1",
  "org.slf4j" % "slf4j-api" % "2.0.16",
  "ch.qos.logback" % "logback-classic" % "1.5.12"
)
