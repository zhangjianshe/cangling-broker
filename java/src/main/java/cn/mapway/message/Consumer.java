package cn.mapway.message;

import cn.mapway.message.proto.AckMessageRequest;
import cn.mapway.message.proto.MessageQueueGrpc;
import cn.mapway.message.proto.QueueMessage;
import cn.mapway.message.proto.SubscribeRequest;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Consumer implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(Consumer.class.getName());

    private final Thread worker;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String consumerId;

    Consumer(
            MessageQueueGrpc.MessageQueueBlockingStub stub,
            SubscribeOptions options,
            String consumerId,
            MessageHandler handler) {
        this.consumerId = consumerId;
        this.worker = new Thread(() -> {
            try {
                Iterator<QueueMessage> stream = stub.subscribe(SubscribeRequest.newBuilder()
                        .setTopic(options.topic())
                        .setConsumerId(consumerId)
                        .build());
                while (!closed.get() && stream.hasNext()) {
                    QueueMessage incoming = stream.next();
                    try {
                        handler.onMessage(toQueueMessage(incoming));
                        stub.ackMessage(AckMessageRequest.newBuilder()
                                .setMessageId(incoming.getMessageId())
                                .setLease(incoming.getLease())
                                .setSuccess(true)
                                .build());
                    } catch (Exception error) {
                        LOG.log(Level.WARNING, "handler failed", error);
                        stub.ackMessage(AckMessageRequest.newBuilder()
                                .setMessageId(incoming.getMessageId())
                                .setLease(incoming.getLease())
                                .setSuccess(false)
                                .setError(error.getMessage() == null ? "handler failed" : error.getMessage())
                                .build());
                    }
                }
            } catch (StatusRuntimeException error) {
                if (!closed.get()) {
                    LOG.log(Level.WARNING, "subscribe stream closed", error);
                }
            }
        }, "cangling-subscribe");
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

    private static cn.mapway.message.QueueMessage toQueueMessage(QueueMessage incoming) {
        cn.mapway.message.QueueMessage message = new cn.mapway.message.QueueMessage();
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
