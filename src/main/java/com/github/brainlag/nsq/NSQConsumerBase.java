package com.github.brainlag.nsq;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.NSQFrame;

/**
 * @author rukyzhc@gmail.com
 */
public abstract class NSQConsumerBase implements Consumer<NSQMessage>,
        AutoCloseable {

    protected final String topic;
    protected int messagesPerBatch = 200;
    protected final String channel;
    protected final NSQErrorCallback errorCallback;
    protected final NSQConfig config;
    protected final NSQMessageCallback callback;
    protected ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor();
    protected ExecutorService executor = Executors.newCachedThreadPool();

    protected boolean started = false;
    private volatile long nextTimeout = 0;
    private Optional<ScheduledFuture<?>> timeout = Optional.empty();
    private final AtomicLong totalMessages = new AtomicLong(0l);

    protected NSQConsumerBase(String topic, String channel,
            NSQMessageCallback callback, NSQConfig config,
            NSQErrorCallback errorCallback) {
        this.channel = channel;
        this.config = config;
        this.callback = callback;
        this.errorCallback = errorCallback;
        this.topic = topic;
    }

    public abstract NSQConsumerBase start();

    protected Connection createConnection(final ServerAddress serverAddress) {
        try {
            final Connection connection = new Connection(serverAddress, config);

            connection.setConsumer(this);
            connection.setErrorCallback(errorCallback);
            connection.command(NSQCommand.instance("SUB " + topic + " "
                    + this.channel));
            connection.command(NSQCommand.instance("RDY " + messagesPerBatch));

            return connection;
        } catch (final NoConnectionsException e) {
            LogManager.getLogger(this).warn(
                    "Could not create connection to server {}",
                    serverAddress.toString(), e);
            return null;
        }
    }

    public void accept(final NSQMessage message) {
        if (callback == null) {
            LogManager.getLogger(this).warn(
                    "NO Callback, dropping message: " + message);
        } else {
            try {
                executor.execute(() -> callback.message(message));
                if (nextTimeout > 0) {
                    updateTimeout(message, -500);
                }
            } catch (RejectedExecutionException re) {
                LogManager.getLogger(this).trace("Backing off");
                message.requeue();
                updateTimeout(message, 500);
            }
        }

        final long tot = totalMessages.incrementAndGet();
        if (tot % messagesPerBatch > (messagesPerBatch / 2)) {
            // request some more!
            rdy(message, messagesPerBatch);
        }
    }

    protected void rdy(final NSQMessage message, int size) {
        message.getConnection().command(NSQCommand.instance("RDY " + size));
    }

    private void updateTimeout(final NSQMessage message, long change) {
        rdy(message, 0);
        LogManager.getLogger(this).trace("RDY 0! Halt Flow.");
        if (timeout.isPresent()) {
            timeout.get().cancel(true);
        }
        Date newTimeout = calculateTimeoutDate(change);
        if (newTimeout != null) {
            timeout = Optional.of(scheduler.schedule(() -> {
                rdy(message, 1); // test the waters
                }, 0, TimeUnit.MILLISECONDS));
        }
    }

    public NSQConsumerBase setMessagesPerBatch(final int messagesPerBatch) {
        if (!started) {
            this.messagesPerBatch = messagesPerBatch;
        }
        return this;
    }

    private Date calculateTimeoutDate(final long i) {
        if (System.currentTimeMillis() - nextTimeout + i > 50) {
            nextTimeout += i;
            return new Date();
        } else {
            nextTimeout = 0;
            return null;
        }
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    /**
     * This method allows for a runnable task to be scheduled using the
     * NSQConsumer's scheduler executor This is intended for calling a periodic
     * method in a NSQMessageCallback for batching messages without needing
     * state in the callback itself
     *
     * @param task
     *            The Runnable task
     * @param delay
     *            Delay in milliseconds
     * @param period
     *            Period of time between scheduled runs
     * @param unit
     *            TimeUnit for delay and period times
     * @return ScheduledFuture - useful for cancelling scheduled task
     */
    public ScheduledFuture scheduleRun(Runnable task, int delay, int period,
            TimeUnit unit) {
        return scheduler.scheduleAtFixedRate(task, delay, period, unit);
    }

    /**
     * Executor where scheduled callback methods are sent to
     *
     * @param scheduler
     *            scheduler to use (defaults to SingleThreadScheduledExecutor)
     * @return this NSQConsumerBase
     */
    public NSQConsumerBase setScheduledExecutor(
            final ScheduledExecutorService scheduler) {
        if (!started) {
            this.scheduler = scheduler;
        }
        return this;
    }

    /**
     * This is the executor where the callbacks happen. The executer can only
     * changed before the client is started. Default is a cached threadpool.
     */
    public NSQConsumerBase setExecutor(final ExecutorService executor) {
        if (!started) {
            this.executor = executor;
        }
        return this;
    }

    protected void cleanClose(Connection connection) throws TimeoutException {
        final NSQCommand command = NSQCommand.instance("CLS");
        final NSQFrame frame = connection.commandAndWait(command);
        if (frame != null && frame instanceof ErrorFrame) {
            final String err = ((ErrorFrame) frame).getErrorMessage();
            if (err.startsWith("E_INVALID")) {
                throw new IllegalStateException(err);
            }
        }
    }

}
