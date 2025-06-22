scalaVersion := "2.13.16"

name         := "tm_data"
organization := "tm_data"
version      := "1.0"

scalacOptions += "-deprecation"

libraryDependencies ++= List(
  // random lihaoyi goodness
  "com.lihaoyi" %% "requests" % "0.9.0",
  "com.lihaoyi" %% "pprint"   % "0.9.0",
  "com.lihaoyi" %% "ujson"    % "4.2.1",
  // csv parsing
  "org.apache.commons" % "commons-csv" % "1.14.0",
  // logging
  "org.slf4j"      % "slf4j-api"       % "2.0.17",
  "ch.qos.logback" % "logback-classic" % "1.5.18",
  "ch.qos.logback" % "logback-core"    % "1.5.18",
  // database handling
  "com.zaxxer"           % "HikariCP"                  % "6.3.0",
  "com.typesafe.slick"  %% "slick"                     % "3.6.1",
  "com.typesafe.slick"  %% "slick-hikaricp"            % "3.6.1",
  "com.h2database"       % "h2"                        % "2.3.232",
  "com.nrinaudo"        %% "kantan.csv"                % "0.8.0",
  "com.nrinaudo"        %% "kantan.csv-generic"        % "0.8.0",
  "org.postgresql"       % "postgresql"                % "42.7.7",
  "software.amazon.jdbc" % "aws-advanced-jdbc-wrapper" % "2.6.0"
)
