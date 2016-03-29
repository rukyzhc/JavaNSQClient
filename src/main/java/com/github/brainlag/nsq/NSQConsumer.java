package com.github.brainlag.nsq;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NSQConsumer extends NSQConsumerBase {

    private final NSQLookup lookup;
    private final Map<ServerAddress, Connection> connections = Maps
            .newHashMap();

    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes
                                           // (and clean up non responsive
                                           // nodes)

    public NSQConsumer(final NSQLookup lookup, final String topic,
            final String channel, final NSQMessageCallback callback) {
        this(lookup, topic, channel, callback, new NSQConfig());
    }

    public NSQConsumer(final NSQLookup lookup, final String topic,
            final String channel, final NSQMessageCallback callback,
            final NSQConfig config) {
        this(lookup, topic, channel, callback, config, null);
    }

    public NSQConsumer(final NSQLookup lookup, final String topic,
            final String channel, final NSQMessageCallback callback,
            final NSQConfig config, final NSQErrorCallback errCallback) {
        super(topic, channel, callback, config, errCallback);
        this.lookup = lookup;
    }

    @Override
    public NSQConsumer start() {
        if (!started) {
            started = true;
            // connect once otherwise we might have to wait one lookupPeriod
            connect();
            scheduler.scheduleAtFixedRate(() -> {
                connect();
            }, lookupPeriod, lookupPeriod, TimeUnit.MILLISECONDS);
        }
        return this;
    }

    protected void cleanClose() {
        try {
            for (final Connection connection : connections.values()) {
                closeConnection(connection);
            }
        } catch (final TimeoutException e) {
            LogManager.getLogger(this).warn("No clean disconnect", e);
        }
    }

    public NSQConsumer setLookupPeriod(final long periodMillis) {
        if (!started) {
            this.lookupPeriod = periodMillis;
        }
        return this;
    }

    private void connect() {
        for (final Iterator<Map.Entry<ServerAddress, Connection>> it = connections
                .entrySet().iterator(); it.hasNext();) {
            if (!it.next().getValue().isConnected()) {
                it.remove();
            }
        }

        final Set<ServerAddress> newAddresses = lookupAddresses();
        final Set<ServerAddress> oldAddresses = connections.keySet();

        LogManager.getLogger(this).debug(
                "Addresses NSQ connected to: " + newAddresses);
        if (newAddresses.isEmpty()) {
            // in case the lookup server is not reachable for a short time we
            // don't we dont want to
            // force close connection
            // just log a message and keep moving
            LogManager.getLogger(this).warn(
                    "No NSQLookup server connections or topic does not exist.");
        } else {
            for (final ServerAddress server : Sets.difference(oldAddresses,
                    newAddresses)) {
                LogManager.getLogger(this).info(
                        "Remove connection " + server.toString());
                connections.get(server).close();
                connections.remove(server);
            }

            for (final ServerAddress server : Sets.difference(newAddresses,
                    oldAddresses)) {
                if (!connections.containsKey(server)) {
                    final Connection connection = createConnection(server);
                    if (connection != null) {
                        connections.put(server, connection);
                    }
                }
            }
        }
    }

    private Set<ServerAddress> lookupAddresses() {
        return lookup.lookup(topic);
    }

}
