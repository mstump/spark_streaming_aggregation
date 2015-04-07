spark_streaming_aggregation
===========================

Event aggregation with spark streaming. The example includes event aggregation over Kafka or TCP event streams. The instructions are [DSE specific](http://www.datastax.com/download) but this should work on a standalone cluster.

### To build and run the Kafka example
1. Build the assembly ```./sbt/sbt package```
1. Make sure you've got a running spark server and Cassandra node listening on localhost
1. Make sure you've got a running Kafka server on localhost with the topic ```events``` pre-provisioned.
1. Start the Kafka producer ```./sbt/sbt "run-main KafkaProducer"```
1. Submit the assembly to the spark server ```dse spark-submit --class KafkaConsumer ./target/scala-2.10/sparkstreamingaggregation_2.10-0.2.jar```
1. Data will be posted to the C* column families ```demo.event_log``` and ```demo.event_counters```

### To build and run the TCP example
1. Build the assembly ```./sbt/sbt package```
1. Make sure you've got a running spark server and Cassandra node listening on localhost
1. Start the TCP producer ```./sbt/sbt "run-main TcpProducer"```
1. Submit the assembly to the spark server ```dse spark-submit --class TcpConsumer ./target/scala-2.10/sparkstreamingaggregation_2.10-0.2.jar```
1. Data will be posted to the C* column families ```demo.event_log``` and ```demo.event_counters```

### "java.lang.NoSuchMethodException" exception
If you get the exception ```java.lang.NoSuchMethodException``` follow [this guide](https://support.datastax.com/entries/78731079--java-lang-NoSuchMethodException-seen-when-attempting-Spark-streaming-from-Kafka) to alleviate the problem.


### Example output
```
cqlsh> select * from demo.event_log ;

 name | bucket   | time                     | count
------+----------+--------------------------+-------
  foo | 23544741 | 2014-10-07 05:21:09-0700 |    29
  foo | 23544740 | 2014-10-07 05:20:09-0700 |    29
```

```
cqlsh> select * from demo.event_counters  ;

 name | bucket   | count
------+----------+-------
foo | 23544741 |    29
```
