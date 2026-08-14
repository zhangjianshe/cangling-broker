package cn.mapway.message;

import cn.mapway.message.proto.AcceptMessageRequest;
import cn.mapway.message.proto.AcceptMessageResponse;
import cn.mapway.message.proto.MessageQueueGrpc;
import cn.mapway.message.proto.RegisterRequest;
import cn.mapway.message.proto.UnregisterRequest;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class MessageClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final MessageQueueGrpc.MessageQueueBlockingStub stub;
    private final MessageQueueGrpc.MessageQueueStub async;

    private MessageClient(ManagedChannel channel) {
        this.channel = channel;
        this.stub = MessageQueueGrpc.newBlockingStub(channel);
        this.async = MessageQueueGrpc.newStub(channel);
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

    public String register(String topic, String name, Map<String, String> attributes) {
        return stub.register(RegisterRequest.newBuilder()
                .setTopic(topic)
                .setName(name == null ? "" : name)
                .putAllAttributes(attributes == null ? Map.of() : attributes)
                .build())
                .getConsumerId();
    }

    public void unregister(String consumerId) {
        if (consumerId == null || consumerId.isBlank()) {
            return;
        }
        stub.unregister(UnregisterRequest.newBuilder().setConsumerId(consumerId).build());
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
        CompletableFuture<SendResult> done = new CompletableFuture<>();
        StreamObserver<AcceptMessageRequest> requests = async.acceptMessages(new StreamObserver<>() {
            @Override
            public void onNext(AcceptMessageResponse response) {
                done.complete(new SendResult(response.getMessageId(), response.getDuplicate()));
            }

            @Override
            public void onError(Throwable error) {
                done.completeExceptionally(error);
            }

            @Override
            public void onCompleted() {
                if (!done.isDone()) {
                    done.completeExceptionally(new IllegalStateException("publish stream closed without a response"));
                }
            }
        });
        requests.onNext(AcceptMessageRequest.newBuilder()
                .setTopic(topic)
                .setPayload(ByteString.copyFrom(payload))
                .setIdempotencyKey(idempotencyKey == null ? "" : idempotencyKey)
                .putAllAttributes(attributes == null ? Collections.emptyMap() : attributes)
                .build());
        requests.onCompleted();
        try {
            return done.get(15, TimeUnit.SECONDS);
        } catch (Exception error) {
            throw new IllegalStateException("publish failed", error);
        }
    }

    public Consumer subscribe(String topic, MessageHandler handler) {
        return subscribe(SubscribeOptions.topic(topic).build(), handler);
    }

    public Consumer subscribe(SubscribeOptions options, MessageHandler handler) {
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(handler, "handler");
        String consumerId = options.consumerId();
        if (!options.name().isBlank() || !options.attributes().isEmpty()) {
            consumerId = stub.register(RegisterRequest.newBuilder()
                    .setTopic(options.topic())
                    .setConsumerId(options.consumerId())
                    .setName(options.name())
                    .putAllAttributes(options.attributes())
                    .build())
                    .getConsumerId();
        }
        return new Consumer(stub, options, consumerId, handler);
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
