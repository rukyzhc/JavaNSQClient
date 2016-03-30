package com.github.brainlag.nsq;

import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;

public class NSQDConsumer extends NSQConsumerBase {

    private Connection connection;
    private final ServerAddress serverAddress;
    private static final int DEFAULT_NSQD_PORT = 4150;

    public NSQDConsumer(String nsqd, String topic, String channel,
            NSQMessageCallback callback) {
        this(nsqd, topic, channel, callback, new NSQConfig(), null);
    }

    public NSQDConsumer(String nsqd, String topic, String channel,
            NSQMessageCallback callback, NSQConfig config) {
        this(nsqd, topic, channel, callback, config, null);
    }

    public NSQDConsumer(String nsqd, String topic, String channel,
            NSQMessageCallback callback, NSQConfig config,
            NSQErrorCallback errorCallback) {
        super(topic, channel, callback, config, errorCallback);
        int p = nsqd.lastIndexOf(':');
        int port = p > 0 ? Integer.parseInt(nsqd.substring(p + 1))
                : DEFAULT_NSQD_PORT;
        String addr = p > 0 ? nsqd.substring(0, p) : nsqd;
        serverAddress = new ServerAddress(addr, port);
    }

    @Override
    public NSQConsumerBase start() {
        if (!started) {
            connection = createConnection(serverAddress);
            started = true;
        }
        return this;
    }

    @Override
    protected void cleanClose() {
        try {
            if (started && connection != null) {
                closeConnection(connection);
            }
        } catch (final TimeoutException e) {
            LogManager.getLogger(this).warn("No clean disconnect", e);
        }
    }

}
