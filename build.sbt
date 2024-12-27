scalaVersion := "2.13.15"

name         := "tm_data"
organization := "tm_data"
version      := "1.0"

scalacOptions += "-deprecation"

libraryDependencies ++= List(
  // random lihaoyi goodness
  "com.lihaoyi" %% "requests" % "0.9.0",
  "com.lihaoyi" %% "pprint"   % "0.9.0",
  "com.lihaoyi" %% "ujson"    % "4.0.2",
  // csv parsing
  "org.apache.commons" % "commons-csv" % "1.12.0",
  // logging
  "org.slf4j"      % "slf4j-api"       % "2.0.16",
  "ch.qos.logback" % "logback-classic" % "1.5.15",
  "ch.qos.logback" % "logback-core"    % "1.5.15",
  // database handling
  "com.zaxxer"           % "HikariCP"                  % "6.2.1",
  "com.typesafe.slick"  %% "slick"                     % "3.5.2",
  "com.typesafe.slick"  %% "slick-hikaricp"            % "3.5.2",
  "com.h2database"       % "h2"                        % "2.3.232",
  "com.nrinaudo"        %% "kantan.csv"                % "0.7.0",
  "com.nrinaudo"        %% "kantan.csv-generic"        % "0.7.0",
  "org.postgresql"       % "postgresql"                % "42.2.2",
  "org.slf4j"            % "slf4j-api"                 % "2.0.16",
  "software.amazon.jdbc" % "aws-advanced-jdbc-wrapper" % "2.5.4"
)
