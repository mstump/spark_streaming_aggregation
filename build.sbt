name := "SparkStreamingAggregation"

version := "0.2"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.10" % "0.8.0",
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.spark" %% "spark-streaming" % "1.1.0",
  "org.apache.spark" %% "spark-sql" % "1.1.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.1.0"
)
