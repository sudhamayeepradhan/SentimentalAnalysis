
name := "sentimentalanalysis"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.3"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0-SNAPSHOT"
)
