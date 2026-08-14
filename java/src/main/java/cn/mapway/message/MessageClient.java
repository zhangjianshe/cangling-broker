package cn.mapway.message;

import cn.mapway.message.proto.AcceptMessageRequest;
import cn.mapway.message.proto.AcceptMessageResponse;
import cn.mapway.message.proto.MessageQueueGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class MessageClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final MessageQueueGrpc.MessageQueueBlockingStub stub;

    private MessageClient(ManagedChannel channel) {
        this.channel = channel;
        this.stub = MessageQueueGrpc.newBlockingStub(channel);
    }

    /**
     * @param broker host:port, for example {@code 127.0.0.1:7500}
     */
    public static MessageClient connect(String broker) {
        Objects.requireNonNull(broker, "broker");
        if (broker.isBlank()) {
            throw new IllegalArgumentException("broker is required");
        }
        String target = broker.startsWith("http://")
                ? broker.substring("http://".length())
                : broker.startsWith("https://") ? broker.substring("https://".length()) : broker;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return new MessageClient(channel);
    }

    public SendResult send(String topic, String utf8Payload) {
        return send(topic, utf8Payload == null ? new byte[0] : utf8Payload.getBytes(StandardCharsets.UTF_8), "", Map.of());
    }

    public SendResult send(String topic, byte[] payload) {
        return send(topic, payload, "", Map.of());
    }

    public SendResult send(String topic, byte[] payload, String idempotencyKey, Map<String, String> attributes) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("payload is required");
        }
        AcceptMessageResponse response = stub.acceptMessage(AcceptMessageRequest.newBuilder()
                .setTopic(topic)
                .setPayload(ByteString.copyFrom(payload))
                .setIdempotencyKey(idempotencyKey == null ? "" : idempotencyKey)
                .putAllAttributes(attributes == null ? Collections.emptyMap() : attributes)
                .build());
        return new SendResult(response.getMessageId(), response.getDuplicate());
    }

    public Consumer subscribe(String topic, String listenHost, int listenPort, MessageHandler handler)
            throws IOException {
        return subscribe(SubscribeOptions.topic(topic).listen(listenHost, listenPort).build(), handler);
    }

    public Consumer subscribe(SubscribeOptions options, MessageHandler handler) throws IOException {
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(handler, "handler");
        return new Consumer(stub, options, handler);
    }

    @Override
    public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
