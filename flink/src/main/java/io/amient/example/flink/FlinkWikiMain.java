/*
 * Copyright 2017 Amient Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.example.flink;

import io.amient.example.irc.IRCMessage;
import io.amient.example.irc.IRCSource;
import io.amient.example.wikipedia.WikipediaMessage;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class FlinkWikiMain {
    private static final Logger log = LoggerFactory.getLogger(FlinkWikiMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WikipediaMessage> wikiEdits = env.addSource(new WikipediaIrcSource(
                "irc.wikimedia.org", 6667, "#en.wikipedia,#en.wiktionary,#en.wikinews"));

        KeyedStream<WikipediaMessage, String> keyedEdits = wikiEdits
                .keyBy((KeySelector<WikipediaMessage, String>) event -> event.user);


        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<String, Long>("", 0L),
                        new FoldFunction<WikipediaMessage, Tuple2<String, Long>>() {
                            public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaMessage event) {
                                acc.f0 = event.title;
                                acc.f1 += event.byteDiff;
                                return acc;
                            }
                        });

        result.print();

        env.execute();

    }

    private static class WikipediaIrcSource extends RichSourceFunction<WikipediaMessage> {
        final private IRCSource ircStream;
        private volatile boolean isRunning = true;

        public WikipediaIrcSource(String host, int port, String channels) {
            //TODO how would parallelisation in flink work here
            ircStream = new IRCSource(host, port, channels.split(","));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ircStream.open();
        }

        @Override
        public void close() throws Exception {
            if (ircStream != null) {
                ircStream.close();
            }
        }

        @Override
        public void run(SourceContext<WikipediaMessage> ctx) throws Exception {
            while (isRunning) {
                IRCMessage irc = ircStream.poll(100, TimeUnit.MILLISECONDS);
                if (irc != null) {
                    try {
                        ctx.collect(WikipediaMessage.parseText(irc.message));
                    } catch (IllegalArgumentException e) {
                        log.warn(e.getMessage());
                    }
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
