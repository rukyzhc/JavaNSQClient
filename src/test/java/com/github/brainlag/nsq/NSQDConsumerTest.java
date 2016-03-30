package com.github.brainlag.nsq;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class NSQDConsumerTest {

    @Test
    public void testMultiMessage() throws NSQException, TimeoutException,
            InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        try (NSQDConsumer consumer = new NSQDConsumer("127.0.0.1:4150",
                "test3", "testconsumer", (message) -> {
                    LogManager.getLogger(this).info(
                            "Processing message: "
                                    + new String(message.getMessage()));
                    counter.incrementAndGet();
                    message.finished();
                })) {
            consumer.start();

            NSQProducer producer = new NSQProducer();
            producer.addAddress("localhost", 4150);
            producer.start();
            List<byte[]> messages = Lists.newArrayList();
            for (int i = 0; i < 50; i++) {
                messages.add(randomString().getBytes());
            }
            producer.produceMulti("test3", messages);
            producer.shutdown();

            while (counter.get() < 50) {
                Thread.sleep(500);
            }
            assertTrue(counter.get() >= 50);
        }
    }

    @Test
    public void testProduceMoreMsg() throws NSQException, TimeoutException,
            InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        try (NSQDConsumer consumer = new NSQDConsumer("127.0.0.1:4150",
                "test3", "testconsumer", (message) -> {
                    LogManager.getLogger(this).info(
                            "Processing message: "
                                    + new String(message.getMessage()));
                    counter.incrementAndGet();
                    message.finished();
                })) {
            consumer.start();

            NSQProducer producer = new NSQProducer();
            producer.addAddress("localhost", 4150);
            producer.start();
            for (int i = 0; i < 1000; i++) {
                String msg = randomString();
                producer.produce("test3", msg.getBytes());
            }
            producer.shutdown();

            while (counter.get() < 1000) {
                Thread.sleep(500);
            }
            assertTrue(counter.get() >= 1000);
        }
    }

    @Test
    public void testParallelProducer() throws NSQException, TimeoutException,
            InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        try (NSQDConsumer consumer = new NSQDConsumer("127.0.0.1:4150",
                "test3", "testconsumer", (message) -> {
                    LogManager.getLogger(this).info(
                            "Processing message: "
                                    + new String(message.getMessage()));
                    counter.incrementAndGet();
                    message.finished();
                })) {
            consumer.start();

            NSQProducer producer = new NSQProducer();
            producer.addAddress("localhost", 4150);
            producer.start();
            for (int n = 0; n < 5; n++) {
                new Thread(() -> {
                    for (int i = 0; i < 1000; i++) {
                        String msg = randomString();
                        try {
                            producer.produce("test3", msg.getBytes());
                        } catch (NSQException | TimeoutException e) {
                            Throwables.propagate(e);
                        }
                    }
                }).start();
            }
            while (counter.get() < 5000) {
                Thread.sleep(500);
            }
            assertTrue(counter.get() >= 5000);
            producer.shutdown();
        }
    }

    private String randomString() {
        return "Message" + new Date().getTime();
    }
}
