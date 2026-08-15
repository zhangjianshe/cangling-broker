package cn.mapway.broker;

import cn.mapway.broker.proto.SatwayMessage;
import cn.mapway.broker.proto.SubscribeRequest;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Consumer implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(Consumer.class.getName());

    private final SatwayClient client;
    private final Thread worker;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String consumerId;

    Consumer(SatwayClient client, SubscribeOptions options, String consumerId, MessageHandler handler) {
        this.client = client;
        this.consumerId = consumerId;
        this.worker = new Thread(() -> run(options, handler), "cangling-subscribe");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public String consumerId() {
        return consumerId;
    }

    @Override
    public void close() {
        closed.set(true);
        worker.interrupt();
    }

    private void run(SubscribeOptions options, MessageHandler handler) {
        long backoffMs = SatwayClient.INITIAL_BACKOFF_MS;
        while (running()) {
            try {
                if (!client.awaitReady()) {
                    return;
                }
                if (!running()) {
                    return;
                }
                client.ensureRegistered(options, consumerId);
                Iterator<SatwayMessage> stream = client.blockingStub()
                        .subscribe(SubscribeRequest.newBuilder()
                                .setTopic(options.topic())
                                .setConsumerId(consumerId == null ? "" : consumerId)
                                .build());
                backoffMs = SatwayClient.INITIAL_BACKOFF_MS;
                while (running() && stream.hasNext()) {
                    SatwayMessage incoming = stream.next();
                    try {
                        handler.onMessage(toSatwayMessage(incoming));
                        client.ack(incoming.getMessageId(), incoming.getLease(), true, "");
                    } catch (Exception error) {
                        LOG.log(Level.WARNING, "handler failed", error);
                        client.ack(
                                incoming.getMessageId(),
                                incoming.getLease(),
                                false,
                                error.getMessage() == null ? "handler failed" : error.getMessage());
                    }
                }
                if (running()) {
                    LOG.info("subscribe stream ended, reconnecting");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (StatusRuntimeException error) {
                if (running()) {
                    LOG.log(Level.WARNING, "subscribe stream closed, reconnecting", error);
                }
            } catch (RuntimeException error) {
                if (running()) {
                    LOG.log(Level.WARNING, "subscribe failed, reconnecting", error);
                } else {
                    return;
                }
            }
            if (running()) {
                client.sleepBackoff(backoffMs);
                backoffMs = Math.min(backoffMs * 2, SatwayClient.MAX_BACKOFF_MS);
            }
        }
    }

    private boolean running() {
        return !closed.get() && client.isOpen();
    }

    private static cn.mapway.broker.SatwayMessage toSatwayMessage(SatwayMessage incoming) {
        cn.mapway.broker.SatwayMessage message = new cn.mapway.broker.SatwayMessage();
        message.setId(incoming.getMessageId());
        message.setTopic(incoming.getTopic());
        message.setPayload(incoming.getPayload().toStringUtf8());
        message.setPayloadEncoding("utf-8");
        message.setAttributes(incoming.getAttributesMap());
        message.setCreatedAt(incoming.getCreatedAt());
        if (!incoming.getPayload().isValidUtf8()) {
            message.setPayload(java.util.Base64.getEncoder().encodeToString(incoming.getPayload().toByteArray()));
            message.setPayloadEncoding("base64");
        } else {
            message.setPayload(new String(incoming.getPayload().toByteArray(), StandardCharsets.UTF_8));
        }
        return message;
    }
}
