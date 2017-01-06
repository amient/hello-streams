/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.example.kstream;

import io.amient.example.irc.IRCMessage;
import io.amient.example.irc.IRCSource;
import io.amient.example.wikipedia.WikipediaMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This is an equivalent of hello-samza project which transforms wikipedia irc channel events into wikipedia-raw
 * and does some basic mapping and stateful aggregation of stats over this record stream.
 * <p>
 * The pipeline consisting of 3 components which are roughly equivalents of all hello-samza tasks.
 * <p>
 * 1.   Wikipedia Feed - Kafka Connect Embedded implementation for connecting external data and publishing them into
 * kafka topic `wikipdia-raw`. Creates maximum 3 tasks defined by the 3 channels subscribed.
 * <p>
 * 2.1. Wikipedia Edit Parser - KStream-KStream mapper which parses incoming `wikipedia-raw` messages, parses them and
 * publishes the result into `wikipedia-parsed` record stream. Published messages are formatted as json.
 * <p>
 * 2.2. Aggregate Number of Edits per Use - Stateful per-user counter.
 */

public class KStreamsWikiMain {

    private static final Logger log = LoggerFactory.getLogger(KStreamsWikiMain.class);

    private static final String BOOTSTRAP_SERVERS = "PLAINTEXT://localhost:9092";

    static volatile boolean isRunning = true;

    public static void main(String[] args) throws Exception {

        //1. Launch Custom External Connector for ingesting Wikipedia IRC feed into wikipedia-raw topic
        String host = "irc.wikimedia.org";
        int port = 6667;
        String channels = "#en.wikipedia,#en.wiktionary,#en.wikinews";


        Thread irc = new Thread() {
            final KafkaProducer<String, String> sourceProducer = new KafkaProducer<>(new Properties() {{
                put("bootstrap.servers", BOOTSTRAP_SERVERS);
                put("key.serializer", StringSerializer.class.getName());
                put("value.serializer", StringSerializer.class.getName());
            }});
            final IRCSource ircStream = new IRCSource(host, port, channels.split(","));

            @Override
            public void run() {
                ircStream.open();
                while (isRunning) try {
                    IRCMessage ircMsg = ircStream.poll(100, TimeUnit.MILLISECONDS);
                    if (ircMsg != null && ircMsg.message != null && ircMsg.user != null) {
                        try {
                            sourceProducer.send(new ProducerRecord<>("wikipedia-raw", ircMsg.message));
                        } catch (IllegalArgumentException e) {
                            log.warn(e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
                sourceProducer.close();
            }
        };
        irc.start();

        //2. Launch Kafka Streams Topology
        KafkaStreams streams = createWikipediaStreamsInstance(BOOTSTRAP_SERVERS);
        try {
            streams.start();
        } catch (Throwable e) {
            log.error("Stopping the application due to streams initialization error ", e);
            isRunning = false;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                isRunning = false;
            }
        });

        try {
            irc.join();
            log.info("Connect closed cleanly...");
        } finally {
            streams.close();
            log.info("Streams closed cleanly...");
        }
    }


    private static KafkaStreams createWikipediaStreamsInstance(String bootstrapServers) {
        final Serde<String> stringSerde = Serdes.String();

        KStreamBuilder builder = new KStreamBuilder();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikipedia-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);


        KStream<String, String> wikipediaRaw = builder.stream(stringSerde, stringSerde, "wikipedia-raw");

        KStream<String, WikipediaMessage> editsParsed = wikipediaRaw.map(
                (String user, String message) -> {
                    try {
                        WikipediaMessage msg = WikipediaMessage.parseText(message);
                        return new KeyValue<>(msg.user, msg);
                    } catch (IllegalArgumentException e) {
                        return new KeyValue<>(user, null);
                    }
                }).filter((key, value) -> value != null);

//        KTable<String, Long> totalEditsByUser = editsParsed
//                .filter((key, value) -> value.type == WikipediaMessage.Type.EDIT)
//                .groupByKey().aggregate(
//                        () -> 0L,
//                        (aggKey, value, agg) -> agg + 1L,
//                        //TimeWindows.of(5000).advanceBy(1000L),
//                        Serdes.Long(),
//                        "wikipedia-edits-by-user"
//                );

//        some print
        editsParsed.
//        totalEditsByUser.toStream().
                process(() -> new AbstractProcessor<String, WikipediaMessage>() {
            @Override
            public void process(String user, WikipediaMessage numEdits) {
                System.out.println("USER: " + user + " num.edits: " + numEdits);
            }
        });

        return new KafkaStreams(builder, props);

    }

}
