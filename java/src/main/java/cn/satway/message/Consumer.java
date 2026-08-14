package cn.satway.message;

import cn.satway.message.proto.MessageQueueGrpc;
import cn.satway.message.proto.RegisterRequest;
import cn.satway.message.proto.UnregisterRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Consumer implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(Consumer.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MessageQueueGrpc.MessageQueueBlockingStub stub;
    private final SubscribeOptions options;
    private final HttpServer server;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<String> consumerId = new AtomicReference<>("");
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Consumer(MessageQueueGrpc.MessageQueueBlockingStub stub, SubscribeOptions options, MessageHandler handler)
            throws IOException {
        this.stub = stub;
        this.options = options;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "cangling-consumer-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        this.server = HttpServer.create(new InetSocketAddress(options.listenHost(), options.listenPort()), 0);
        this.server.createContext("/messages", exchange -> {
            try {
                if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                byte[] body = exchange.getRequestBody().readAllBytes();
                QueueMessage message = MAPPER.readValue(body, QueueMessage.class);
                handler.onMessage(message);
                byte[] ok = "accepted".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(202, ok.length);
                exchange.getResponseBody().write(ok);
            } catch (Exception error) {
                LOG.log(Level.WARNING, "handler failed for incoming message", error);
                byte[] text = error.getMessage() == null
                        ? new byte[0]
                        : error.getMessage().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(500, text.length);
                if (text.length > 0) {
                    exchange.getResponseBody().write(text);
                }
            } finally {
                exchange.close();
            }
        });
        this.server.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "cangling-consumer-http");
            thread.setDaemon(true);
            return thread;
        }));
        this.server.start();
        heartbeat();
        long seconds = Math.max(1, options.heartbeat().toSeconds());
        this.scheduler.scheduleAtFixedRate(this::safeHeartbeat, seconds, seconds, TimeUnit.SECONDS);
        LOG.info(() -> "consuming topic=" + options.topic() + " callback=" + options.callbackUrl());
    }

    public String consumerId() {
        return consumerId.get();
    }

    public String callbackUrl() {
        return options.callbackUrl();
    }

    private void safeHeartbeat() {
        if (closed.get()) {
            return;
        }
        try {
            heartbeat();
        } catch (Exception error) {
            LOG.log(Level.WARNING, "register heartbeat failed", error);
        }
    }

    private void heartbeat() {
        var response = stub.register(RegisterRequest.newBuilder()
                .setTopic(options.topic())
                .setDownstreamUrl(options.callbackUrl())
                .setConsumerId(consumerId.get())
                .build());
        consumerId.set(response.getConsumerId());
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();
        server.stop(0);
        String id = consumerId.get();
        if (id != null && !id.isBlank()) {
            try {
                stub.unregister(UnregisterRequest.newBuilder().setConsumerId(id).build());
            } catch (Exception error) {
                LOG.log(Level.FINE, "unregister failed", error);
            }
        }
    }
}
