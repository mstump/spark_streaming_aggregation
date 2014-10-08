spark_streaming_aggregation
===========================

Event aggregation with spark streaming

### To build and run
1. Build the assembly ```./sbt/sbt package```
1. Make sure you've got a running spark server and Cassandra node listening on localhost
1. Start netcat on port 9999 ```nc -lk 9999```
1. Submit the assembly to the spark server ```dse spark-submit --class Test ./target/scala-2.10/sparkstreamingaggregation_2.10-0.1.jar```
1. Post sample records by pasting them to the terminal running netcat ```2014-10-07T12:20:09Z;foo;1```
1. Data will be posted to the C* column families ```demo.event_log``` and ```demo.event_counters```

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
