scalaVersion := "2.13.12"

name := "tm_data"
organization := "com.fieldmark"
version := "1.0"

libraryDependencies ++= List(
  "com.lihaoyi" %% "requests" % "0.9.0",
  "com.lihaoyi" %% "pprint" % "0.9.0",
  "com.lihaoyi" %% "ujson" % "4.0.0",
  "org.apache.commons" % "commons-csv" % "1.12.0"
)
