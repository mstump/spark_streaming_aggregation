//
// to stream data into the cluster open up netcat and echo sample records to it, one per line.
//
// nc -lk 9999
// 2014-10-07T12:20:09Z;foo;1
// 2014-10-07T12:21:09Z;foo;29
// 2014-10-07T12:22:10Z;foo;1
// 2014-10-07T12:23:11Z;foo;29

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import java.util.Date
import javax.xml.bind.DatatypeConverter

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.utils.UUIDs


object Test {

  case class Record(bucket:Long, time:Date, name:String, count:Long)
  case class RecordCount(bucket:Long, name:String, count:Long)

  val sampleRecords = Array("2014-10-07T12:20:08Z;foo;1", "2014-10-07T12:20:09Z;foo;1",
                            "2014-10-07T12:20:10Z;foo;1", "2014-10-07T12:20:08Z;bar;1",
                            "2014-10-07T12:20:09Z;bar;3", "2014-10-07T12:21:09Z;bar;1")

  def parseDate(str:String) : Date = {
    return javax.xml.bind.DatatypeConverter.parseDateTime(str).getTime()
  }

  def minuteBucket(d:Date) : Long = {
    return d.getTime() / (60 * 1000)
  }

  def parseMessage(msg:String) : Record = {
    val arr = msg.split(";")
    val time = parseDate(arr(0))
    return Record(minuteBucket(time), time, arr(1), arr(2).toInt)
  }

  def createSchema(cc:CassandraConnector, keySpaceName:String, counters:String, logs:String) = {
    cc.withSessionDo { session =>
      session.execute(s"DROP TABLE IF EXISTS ${keySpaceName}.${logs};")
      session.execute(s"DROP TABLE IF EXISTS ${keySpaceName}.${counters};")

      session.execute("CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.${logs} (name text, bucket bigint, count bigint, time timestamp, " +
                      s"PRIMARY KEY((name, bucket), time));")

      session.execute("CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.${counters} (name text, bucket bigint, count counter, " +
                      s"PRIMARY KEY(name, bucket));")
    }
  }

  def main(args: Array[String]) {
    val sparkMasterHost = "127.0.0.1"
    val cassandraHost = "127.0.0.1"
    val cassandraKeyspace = "demo"
    val cassandraCfCounters = "event_counters"
    val cassandraCfEvents = "event_log"

    // Tell Spark the address of one Cassandra node:
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[12]")
      .setAppName(getClass.getSimpleName)

    // Connect to the Spark cluster:
    lazy val sc = new SparkContext(conf)
    lazy val ssc = new StreamingContext(sc, Seconds(1))

    // https://twitter.com/pwendell/status/580242656082546688
    sys.ShutdownHookThread {
      log.info("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      log.info("Application stopped.")
    }

    lazy val cc = CassandraConnector(sc.getConf)
    createSchema(cc, cassandraKeyspace, cassandraCfCounters, cassandraCfEvents)

    // for testing purposes you can use the alternative input below
    // val input = sc.parallelize(sampleRecords)
    val input = ssc.socketTextStream("localhost", 9999)
    val parsedRecords = input.map(parseMessage)
    val bucketedRecords = parsedRecords.map(record => ((record.bucket, record.name), record))
    val bucketedCounts = bucketedRecords.combineByKey(
      (record) => record.count,
      (count:Long, record:Record) => (count + record.count),
      (c1:Long, c2:Long) => (c1 + c2),
      new HashPartitioner(1))

    val flattenCounts = bucketedCounts.map((agg) => RecordCount(agg._1._1, agg._1._2, agg._2))

    parsedRecords.print()
    parsedRecords.saveToCassandra(cassandraKeyspace, cassandraCfEvents)
    flattenCounts.saveToCassandra(cassandraKeyspace, cassandraCfCounters)

    ssc.start()
    ssc.awaitTermination()
  }
}
