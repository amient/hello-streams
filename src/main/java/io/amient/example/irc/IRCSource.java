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

package io.amient.example.irc;

import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventAdapter;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class IRCSource implements Closeable, Serializable {

    private static final Logger log = LoggerFactory.getLogger(IRCSource.class);

    private final String host;
    private final int port;
    private final String[] channels;
    private BlockingQueue<IRCMessage> queue = null;
    private IRCConnection conn;

    public IRCSource(String host, int port, String[] channels) {
        this.host = host;
        this.port = port;
        this.channels = channels;
    }



    public void open() {
        queue = new LinkedBlockingQueue<>();

        String nick = "kafka-connect-irc-" + Math.abs(new Random().nextInt());
        log.info("Starting irc feed task " + nick + ", channels " + String.join(",", channels));
        this.conn = new IRCConnection(host, new int[]{port}, "", nick, nick, nick);
        this.conn.addIRCEventListener(new IRCMessageListener());
        this.conn.setEncoding("UTF-8");
        this.conn.setPong(true);
        this.conn.setColors(false);
        try {
            this.conn.connect();
        } catch (IOException e) {
            throw new RuntimeException("Unable to connect to " + host + ":" + port + ".", e);
        }
        for (String channel : channels) {
            this.conn.send("JOIN " + channel);
        }
    }

    @Override
    public void close() {

    }

    public IRCMessage poll(int timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    class IRCMessageListener extends IRCEventAdapter {
        @Override
        public void onPrivmsg(String channel, IRCUser u, String msg) {
            queue.offer(new IRCMessage(channel, u, msg));
        }

        @Override
        public void onError(String msg) {
            log.warn("IRC Error: " + msg);
        }
    }
}
