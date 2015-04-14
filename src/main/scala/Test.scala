//
// to stream data into the cluster open up netcat and echo sample records to it, one per line.
//
// nc -lk 9999
// 2014-10-07T12:20:09Z;foo;1
// 2014-10-07T12:21:09Z;foo;29
// 2014-10-07T12:22:10Z;foo;1
// 2014-10-07T12:23:11Z;foo;29

import java.net._
import java.io._
import scala.io._

import java.util.Properties
import java.util.Date
import java.util.Random
import java.util.TimeZone
import javax.xml.bind.DatatypeConverter

import kafka.producer.{ProducerConfig, KeyedMessage, Producer}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.utils.UUIDs

object Config {
  val sparkMasterHost = "127.0.0.1"
  val cassandraHost = "127.0.0.1"
  val cassandraKeyspace = "demo"
  val cassandraCfCounters = "event_counters"
  val cassandraCfEvents = "event_log"
  val zookeeperHost = "localhost:2181"
  val kafkaHost = "localhost:9092"
  val kafkaTopic = "events"
  val kafkaConsumerGroup = "spark-streaming-test"
  val tcpHost = "localhost"
  val tcpPort = 9999
}

case class Record(bucket:Long, time:Date, name:String, count:Long)

case class RecordCount(bucket:Long, name:String, count:Long)

object StreamConsumer {

  def setup() : (SparkContext, StreamingContext, CassandraConnector) = {
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", Config.cassandraHost)
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[12]")
      .setAppName(getClass.getSimpleName)

    // Connect to the Spark cluster:
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val cc = CassandraConnector(sc.getConf)
    createSchema(cc, Config.cassandraKeyspace, Config.cassandraCfCounters, Config.cassandraCfEvents)
    return (sc, ssc, cc)
  }

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

  def process(ssc : StreamingContext, input : DStream[String]) {
    // for testing purposes you can use the alternative input below
    // val input = sc.parallelize(sampleRecords)
    val parsedRecords = input.map(parseMessage)
    val bucketedRecords = parsedRecords.map(record => ((record.bucket, record.name), record))
    val bucketedCounts = bucketedRecords.combineByKey(
      (record:Record) => record.count,
      (count:Long, record:Record) => (count + record.count),
      (c1:Long, c2:Long) => (c1 + c2),
      new HashPartitioner(1))

    val flattenCounts = bucketedCounts.map((agg) => RecordCount(agg._1._1, agg._1._2, agg._2))

    parsedRecords.print()
    parsedRecords.saveToCassandra(Config.cassandraKeyspace, Config.cassandraCfEvents)
    flattenCounts.saveToCassandra(Config.cassandraKeyspace, Config.cassandraCfCounters)

    // https://twitter.com/pwendell/status/580242656082546688
    sys.ShutdownHookThread {
      ssc.stop(true, true)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

object KafkaConsumer {
  def main(args: Array[String]) {
    val (sc, ssc, cc) = StreamConsumer.setup()
    val input = KafkaUtils.createStream(
      ssc,
      Config.zookeeperHost,
      Config.kafkaConsumerGroup,
      Map(Config.kafkaTopic -> 1)).map(_._2)
    StreamConsumer.process(ssc, input)
  }
}

object TcpConsumer {
  def main(args: Array[String]) {
    val (sc, ssc, cc) = StreamConsumer.setup()
    val input = ssc.socketTextStream(Config.tcpHost, Config.tcpPort)
    StreamConsumer.process(ssc, input)
  }
}

object EventGenerator {

  val eventNames = Array("thyrotome", "radioactivated", "toreutics", "metrological",
    "adelina", "architecturally", "unwontedly", "histolytic", "clank", "unplagiarised",
    "inconsecutive", "scammony", "pelargonium", "preaortic", "goalmouth", "adena",
    "murphy", "vaunty", "confetto", "smiter", "chiasmatype", "fifo", "lamont", "acnode",
    "mutating", "unconstrainable", "donatism", "discept")

  def currentTimestamp() : String = {
    val tz = TimeZone.getTimeZone("UTC")
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    sdf.setTimeZone(tz)
    val dateString = sdf.format(new java.util.Date)
    return dateString
  }

  def randomEventName() : String = {
    val rand = new Random(System.currentTimeMillis())
    val random_index = rand.nextInt(eventNames.length)
    return eventNames(random_index)
  }

  def generateEvent() : String = {
    // message in the form of "2014-10-07T12:20:08Z;foo;1"
    val eventCount = scala.util.Random.nextInt(10).toString()
    return currentTimestamp() + ";" + randomEventName() + ";" + eventCount
  }
}

object KafkaProducer {
 def main(args: Array[String]) {
   val props = new Properties()
   props.put("metadata.broker.list", Config.kafkaHost)
   props.put("serializer.class", "kafka.serializer.StringEncoder")

   val config = new ProducerConfig(props)
   val producer = new Producer[String, String](config)

   while(true) {
     val event = EventGenerator.generateEvent();
     println(event)
     producer.send(new KeyedMessage[String, String](Config.kafkaTopic, event))
     Thread.sleep(100)
   }
 }
}

object TcpProducer {
  def main(args: Array[String]) {
    val server = new ServerSocket(Config.tcpPort)

    while (true) {
      val socket = server.accept()
      val outstream = new PrintStream(socket.getOutputStream())
      while (!outstream.checkError()) {
        val event = EventGenerator.generateEvent();
        println(event)
        outstream.println(event)
        Thread.sleep(100)
      }
      socket.close()
    }
  }
}
