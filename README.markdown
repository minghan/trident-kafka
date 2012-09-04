trident-kafka
=============

trident-kafka provides Trident spouts for Apache Kafka 0.7.

Running
-------

    lein deps
    lein compile
    java -cp $(lein classpath) trident.kafka.test.Tester

Todo
----

1. Add blacklisting of Kafka servers in `OpaqueTridentKafkaSpout`
2. Discover Kafka brokers dynamically through Zookeeper instead of using a static list of brokers
3. Discover number of partitions / topic / host dynamically
