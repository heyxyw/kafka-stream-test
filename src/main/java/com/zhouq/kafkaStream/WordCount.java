package com.zhouq.kafkaStream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;



/**
 * @author zhouq
 * @email zhouq@daqsoft.com
 * @date 2017-08-22 12:43
 * @Version:
 * @Describe: 计算单词数量
// * 1.启动zookeeper
// *      linux: cd 到 zookeeper 的bin 目录下 执行 zkServer.sh start
// *      window: cd 到 zookeeper 的bin 目录下 执行 zkServer
// *
// * 2.启动 kafka
// *      linux: bin/kafka-server-start.sh config/server.properties
// *      windows: .\bin\windows\kafka-server-start.bat .\config\server.properties
// *
// * 3.创建 主题 zhouq streams-wordcount-output RekeyedIntermediateTopic (可以不创建)
// *      linux: bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic zhouq
// *      windows: .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic zhouq
// *
// * 4.创建 Producer (消息生产者)
// *      linux: bin/kafka-console-producer.sh --broker-list hadoop1:9092 --topic zhouq
// *      windows: .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic zhouq
// *
// * 5.创建 Consumer(消息消费者)
// *      linux: bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic streams-wordcount-output
// *      window: .\bin\windows\kafka-console-consumer.bat  --zookeeper localhost:2181 --topic streams-wordcount-output
// *
// *   注: 1.如果 streams-wordcount-output 主题 未创建而导致消费错误.请手动创建 streams-wordcount-output
// *       2.下面的计数结果输出到 主题 streams-wordcount-output 后.消费者需要一些格式化参数 才能显示成如下格式:
//*             aaa  1
//*             bbb  2
//*             aaa  3
//*             ccc  1
// *          否则 消费者页面会出现序列化问题.结果不会出现预期格式
// *       3.若要消费者打印如上格式 请使用以下命令
// *          linux:
// *                 bin/kafka-console-consumer.sh --zookeeper localhost:2181 \
//                        --topic streams-wordcount-output \
//                        --from-beginning \
//                        --formatter kafka.tools.DefaultMessageFormatter \
//                        --property print.key=true \
//                        --property print.key=true \
//                        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
//                        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
//
//            windows:
//                    .\bin\windows\kafka-console-consumer.bat  --zookeeper localhost:2181
//                        --topic streams-wordcount-output
//                        --from-beginning
//                        --formatter kafka.tools.DefaultMessageFormatter
//                        --property print.key=true
//                        --property print.key=true
//                        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
//                        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
//
//        4.若当前主题在之前接收消息使用的是默认序列化方式.当新启动的消费者又使用的是上面 第三点中的配置 将会抛出类似如下错误
//            ERROR Unknown error when running consumer:  (kafka.tools.ConsoleConsumer$)
//            org.apache.kafka.common.errors.SerializationException: Size of data received by LongDeserializer is not 8
//
//        解决办法 将数据重新写入一个新开的topic 中.

 *
 *
 *
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException {

        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,"F:\\tmp\\kafka-streams");
        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // In the subsequent lines we define the processing topology of the Streams application.
        KStreamBuilder builder = new KStreamBuilder();

        // Construct a `KStream` from the input topic "TextLinesTopic", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        //
        // Note: We could also just call `builder.stream("TextLinesTopic")` if we wanted to leverage
        // the default serdes specified in the Streams configuration above, because these defaults
        // match what's in the actual topic.  However we explicitly set the deserializers in the
        // call to `stream()` below in order to show how that's done, too.
        KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "zhouq");

//        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KStream<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.  The text lines are the record
                // values, i.e. we can ignore whatever data is in the record keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                // We will subsequently invoke `countByKey` to count the occurrences of words, so we use
                // `map` to ensure the key of each record contains the respective word.
                .map((key, word) -> new KeyValue<>(word, word))
                // Required in 0.10.0 to re-partition the data because we re-keyed the stream in the `map`
                // step.  Upcoming Kafka 0.10.1 does this automatically for you (no need for `through`).
                .through("RekeyedIntermediateTopic")
                // Count the occurrences of each word (record key).
                //
                // This will change the stream type from `KStream<String, String>` to
                // `KTable<String, Long>` (word -> count).  We must provide a name for
                // the resulting KTable, which will be used to name e.g. its associated
                // state store and changelog topic.
                .groupByKey().count("Counts")
                // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
                .toStream();

        // Write the `KStream<String, Long>` to the output topic.
        wordCounts.to(stringSerde, longSerde, "streams-wordcount-output");

        // Now that we have finished the definition of the processing topology we can actually run
        // it via `start()`.  The Streams application as a whole can be launched just like any
        // normal Java application that has a `main()` method.
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
