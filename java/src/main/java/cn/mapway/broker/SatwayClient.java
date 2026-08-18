package cn.mapway.broker;

import cn.mapway.broker.proto.AcceptMessageRequest;
import cn.mapway.broker.proto.AcceptMessageResponse;
import cn.mapway.broker.proto.AckMessageRequest;
import cn.mapway.broker.proto.ConfigureTopicsRequest;
import cn.mapway.broker.proto.ListTopicsRequest;
import cn.mapway.broker.proto.MessageQueueGrpc;
import cn.mapway.broker.proto.RegisterRequest;
import cn.mapway.broker.proto.UnregisterRequest;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Broker client. After {@link #connect(String)}, this object owns the gRPC channel
 * and reconnects on its own: unary RPCs retry with backoff, and each {@link Consumer}
 * reopens its Subscribe stream with the same {@code consumer_id}.
 */
public final class SatwayClient implements AutoCloseable {
    static final long INITIAL_BACKOFF_MS = 200;
    static final long MAX_BACKOFF_MS = 5_000;
    private static final long RPC_DEADLINE_SECS = 15;
    private static final Logger LOG = Logger.getLogger(SatwayClient.class.getName());

    private final ManagedChannel channel;
    private final MessageQueueGrpc.MessageQueueBlockingStub stub;
    private final MessageQueueGrpc.MessageQueueStub async;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final Thread reconnect;
    private final List<Consumer> consumers = new CopyOnWriteArrayList<>();
    private final List<ConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();

    private SatwayClient(ManagedChannel channel, ConnectionListener onConnected) {
        this.channel = channel;
        this.stub = MessageQueueGrpc.newBlockingStub(channel);
        this.async = MessageQueueGrpc.newStub(channel);
        if (onConnected != null) {
            connectionListeners.add(onConnected);
        }
        this.reconnect = new Thread(this::maintainConnection, "satway-reconnect");
        this.reconnect.setDaemon(true);
        this.reconnect.start();
    }

    /**
     * Connects using {@code CL_BROKER_AUTH_TOKEN} from the environment when set.
     *
     * @param broker host:port, for example {@code 127.0.0.1:7500}
     */
    public static SatwayClient connect(String broker) {
        return connect(broker, authTokenFromEnv());
    }

    /**
     * @param broker host:port, for example {@code 127.0.0.1:7500}
     * @param token  shared secret matching the broker {@code CL_BROKER_AUTH_TOKEN}.
     *               Blank skips the header (only works if the broker has no token).
     */
    public static SatwayClient connect(String broker, String token) {
        return connect(broker, token, null);
    }

    /**
     * Same as {@link #connect(String, String)}, and also registers a listener
     * that runs when the channel becomes ready (first connect and every reconnect).
     * Use this to {@link #configureTopics} as soon as the broker is up.
     */
    public static SatwayClient connect(String broker, String token, ConnectionListener onConnected) {
        Objects.requireNonNull(broker, "broker");
        if (broker.isBlank()) {
            throw new IllegalArgumentException("broker is required");
        }
        String target = broker.startsWith("http://")
                ? broker.substring("http://".length())
                : broker.startsWith("https://") ? broker.substring("https://".length()) : broker;
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .idleTimeout(365, TimeUnit.DAYS);
        builder.intercept(clientHeadersInterceptor(token));
        return new SatwayClient(builder.build(), onConnected);
    }

    /**
     * Register a listener for READY. If the channel is already ready, {@code listener}
     * is invoked immediately. Returns {@code this} for chaining.
     */
    public SatwayClient onConnected(ConnectionListener listener) {
        Objects.requireNonNull(listener, "listener");
        connectionListeners.add(listener);
        if (channel.getState(false) == ConnectivityState.READY) {
            notifyConnected(listener);
        }
        return this;
    }

    public boolean connected() {
        return open.get() && channel.getState(false) == ConnectivityState.READY;
    }

    public static String authTokenFromEnv() {
        String token = System.getenv("CL_BROKER_AUTH_TOKEN");
        return token == null ? "" : token.trim();
    }

    public String register(String topic, String name, Map<String, String> attributes) {
        return register(topic, "", name, attributes);
    }

    public java.util.List<TopicConfig> configureTopics(java.util.List<TopicConfig> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("topics is required");
        }
        return callWithReconnect("configureTopics", () -> {
            ConfigureTopicsRequest.Builder request = ConfigureTopicsRequest.newBuilder();
            for (TopicConfig topic : topics) {
                request.addTopics(cn.mapway.broker.proto.TopicConfig.newBuilder()
                        .setTopic(topic.topic())
                        .setDelivery(topic.delivery())
                        .setPersistence(topic.persistence())
                        .build());
            }
            return blockingStub()
                    .withDeadlineAfter(RPC_DEADLINE_SECS, TimeUnit.SECONDS)
                    .configureTopics(request.build())
                    .getTopicsList()
                    .stream()
                    .map(item -> new TopicConfig(item.getTopic(), item.getDelivery(), item.getPersistence()))
                    .collect(Collectors.toList());
        });
    }

    public java.util.List<TopicConfig> listTopics() {
        return callWithReconnect("listTopics", () -> blockingStub()
                .withDeadlineAfter(RPC_DEADLINE_SECS, TimeUnit.SECONDS)
                .listTopics(ListTopicsRequest.getDefaultInstance())
                .getTopicsList()
                .stream()
                .map(item -> new TopicConfig(item.getTopic(), item.getDelivery(), item.getPersistence()))
                .collect(Collectors.toList()));
    }

    public void unregister(String consumerId) {
        if (consumerId == null || consumerId.isBlank()) {
            return;
        }
        callWithReconnect("unregister", () -> {
            blockingStub()
                    .withDeadlineAfter(RPC_DEADLINE_SECS, TimeUnit.SECONDS)
                    .unregister(UnregisterRequest.newBuilder().setConsumerId(consumerId).build());
            return null;
        });
    }

    public SendResult send(String topic, String utf8Payload) {
        return send(topic, utf8Payload == null ? new byte[0] : utf8Payload.getBytes(java.nio.charset.StandardCharsets.UTF_8), "", Map.of());
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
        String key = idempotencyKey == null || idempotencyKey.isBlank()
                ? UUID.randomUUID().toString()
                : idempotencyKey;
        Map<String, String> attrs = attributes == null ? Collections.emptyMap() : attributes;
        byte[] body = payload;
        return callWithReconnect("publish", () -> publishOnce(topic, body, key, attrs));
    }

    public Consumer subscribe(String topic, MessageHandler handler) {
        return subscribe(SubscribeOptions.topic(topic).build(), handler);
    }

    public Consumer subscribe(SubscribeOptions options, MessageHandler handler) {
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(handler, "handler");
        String consumerId = options.consumerId();
        if (!options.name().isBlank() || !options.attributes().isEmpty() || !options.consumerId().isBlank()) {
            consumerId = register(options.topic(), consumerId, options.name(), options.attributes());
        }
        Consumer consumer = new Consumer(this, options, consumerId, handler);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public void close() {
        if (!open.compareAndSet(true, false)) {
            return;
        }
        for (Consumer consumer : consumers) {
            consumer.close();
        }
        reconnect.interrupt();
        channel.shutdownNow();
        try {
            channel.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    boolean isOpen() {
        return open.get();
    }

    MessageQueueGrpc.MessageQueueBlockingStub blockingStub() {
        return stub;
    }

    String register(String topic, String consumerId, String name, Map<String, String> attributes) {
        Map<String, String> attrs = withClientIdentity(attributes);
        return callWithReconnect("register", () -> blockingStub()
                .withDeadlineAfter(RPC_DEADLINE_SECS, TimeUnit.SECONDS)
                .register(RegisterRequest.newBuilder()
                        .setTopic(topic)
                        .setConsumerId(consumerId == null ? "" : consumerId)
                        .setName(name == null ? "" : name)
                        .putAllAttributes(attrs)
                        .build())
                .getConsumerId());
    }

    void ensureRegistered(SubscribeOptions options, String consumerId) {
        if (consumerId == null || consumerId.isBlank()) {
            return;
        }
        register(options.topic(), consumerId, options.name(), options.attributes());
    }

    void ack(String messageId, String lease, boolean success, String error) {
        callWithReconnect("ack", () -> {
            blockingStub()
                    .withDeadlineAfter(RPC_DEADLINE_SECS, TimeUnit.SECONDS)
                    .ackMessage(AckMessageRequest.newBuilder()
                            .setMessageId(messageId)
                            .setLease(lease)
                            .setSuccess(success)
                            .setError(error == null ? "" : error)
                            .build());
            return null;
        });
    }

    boolean awaitReady() throws InterruptedException {
        while (open.get()) {
            ConnectivityState state = channel.getState(true);
            if (state == ConnectivityState.READY) {
                return true;
            }
            if (state == ConnectivityState.SHUTDOWN) {
                return false;
            }
            CountDownLatch changed = new CountDownLatch(1);
            channel.notifyWhenStateChanged(state, changed::countDown);
            changed.await(1, TimeUnit.SECONDS);
        }
        return false;
    }

    void sleepBackoff(long delayMs) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void maintainConnection() {
        ConnectivityState last = null;
        while (open.get()) {
            try {
                ConnectivityState state = channel.getState(true);
                if (state != last) {
                    if (state == ConnectivityState.READY) {
                        LOG.info("connected to broker");
                        notifyConnected();
                    } else if (state != ConnectivityState.SHUTDOWN) {
                        LOG.log(Level.WARNING, "broker {0}, reconnecting", state);
                        if (last == ConnectivityState.READY) {
                            notifyDisconnected();
                        }
                    }
                    last = state;
                }
                CountDownLatch changed = new CountDownLatch(1);
                channel.notifyWhenStateChanged(state, changed::countDown);
                changed.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void notifyConnected() {
        for (ConnectionListener listener : connectionListeners) {
            notifyConnected(listener);
        }
    }

    private void notifyConnected(ConnectionListener listener) {
        try {
            listener.onConnected(this);
        } catch (Exception error) {
            LOG.log(Level.WARNING, "connection listener failed", error);
        }
    }

    private void notifyDisconnected() {
        for (ConnectionListener listener : connectionListeners) {
            try {
                listener.onDisconnected(this);
            } catch (Exception error) {
                LOG.log(Level.WARNING, "disconnect listener failed", error);
            }
        }
    }

    private SendResult publishOnce(String topic, byte[] payload, String idempotencyKey, Map<String, String> attributes)
            throws Exception {
        CompletableFutureSend done = new CompletableFutureSend();
        StreamObserver<AcceptMessageRequest> requests = async.acceptMessages(done);
        requests.onNext(AcceptMessageRequest.newBuilder()
                .setTopic(topic)
                .setPayload(ByteString.copyFrom(payload))
                .setIdempotencyKey(idempotencyKey)
                .putAllAttributes(attributes)
                .build());
        requests.onCompleted();
        return done.get(RPC_DEADLINE_SECS, TimeUnit.SECONDS);
    }

    private <T> T callWithReconnect(String op, Callable<T> call) {
        long backoffMs = INITIAL_BACKOFF_MS;
        while (true) {
            if (!open.get()) {
                throw new IllegalStateException("client closed");
            }
            try {
                if (!awaitReady()) {
                    throw new IllegalStateException("client closed");
                }
                return call.call();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(op + " interrupted", interrupted);
            } catch (Exception error) {
                if (!open.get()) {
                    throw new IllegalStateException("client closed", error);
                }
                if (!isRetryable(error)) {
                    throw new IllegalStateException(op + " failed", error);
                }
                LOG.log(Level.WARNING, "{0} failed, reconnecting: {1}", new Object[]{op, rootMessage(error)});
                sleepBackoff(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
    }

    private static boolean isRetryable(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof StatusRuntimeException) {
                return isRetryableCode(((StatusRuntimeException) current).getStatus().getCode());
            }
            if (current instanceof java.util.concurrent.TimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isRetryableCode(Status.Code code) {
        return code == Status.Code.UNAVAILABLE
                || code == Status.Code.DEADLINE_EXCEEDED
                || code == Status.Code.ABORTED
                || code == Status.Code.UNKNOWN;
    }

    public static String clientVersion() {
        String version = packagedVersion();
        return version == null || version.isBlank() ? "java" : "java/" + version;
    }

    public static String clientHost() {
        String override = envOrEmpty("CL_BROKER_CLIENT_HOST");
        if (!override.isEmpty()) {
            return override;
        }
        String hostname = envOrEmpty("HOSTNAME");
        if (!hostname.isEmpty()) {
            return hostname;
        }
        try {
            return asciiHeader(InetAddress.getLocalHost().getHostName());
        } catch (Exception ignored) {
            return "";
        }
    }

    private static Map<String, String> withClientIdentity(Map<String, String> attributes) {
        Map<String, String> attrs = new HashMap<>();
        if (attributes != null) {
            attrs.putAll(attributes);
        }
        attrs.putIfAbsent("version", clientVersion());
        String host = clientHost();
        if (!host.isEmpty()) {
            attrs.putIfAbsent("host", host);
        }
        return attrs;
    }

    private static String envOrEmpty(String name) {
        String value = System.getenv(name);
        return value == null ? "" : asciiHeader(value);
    }

    private static String asciiHeader(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c >= 32 && c < 127) {
                out.append(c);
            }
        }
        return out.toString().trim();
    }

    private static String packagedVersion() {
        Package pkg = SatwayClient.class.getPackage();
        if (pkg != null) {
            String fromManifest = pkg.getImplementationVersion();
            if (fromManifest != null && !fromManifest.isBlank()) {
                return fromManifest.trim();
            }
        }
        try (InputStream in = SatwayClient.class.getResourceAsStream(
                "/META-INF/maven/cn.mapway/cangling-broker/pom.properties")) {
            if (in != null) {
                Properties properties = new Properties();
                properties.load(in);
                String fromPom = properties.getProperty("version");
                if (fromPom != null && !fromPom.isBlank()) {
                    return fromPom.trim();
                }
            }
        } catch (IOException ignored) {
        }
        return null;
    }

    private static ClientInterceptor clientHeadersInterceptor(String token) {
        Metadata.Key<String> versionKey =
                Metadata.Key.of("x-client-version", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> hostKey =
                Metadata.Key.of("x-client-host", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> authKey =
                Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        String version = clientVersion();
        String host = clientHost();
        String header = token == null || token.isBlank()
                ? null
                : (token.regionMatches(true, 0, "Bearer ", 0, 7) ? token : "Bearer " + token);
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(versionKey, version);
                        if (!host.isEmpty()) {
                            headers.put(hostKey, host);
                        }
                        if (header != null) {
                            headers.put(authKey, header);
                        }
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null || message.isBlank() ? current.getClass().getSimpleName() : message;
    }

    private static final class CompletableFutureSend
            extends java.util.concurrent.CompletableFuture<SendResult>
            implements StreamObserver<AcceptMessageResponse> {
        @Override
        public void onNext(AcceptMessageResponse response) {
            complete(new SendResult(response.getMessageId(), response.getDuplicate()));
        }

        @Override
        public void onError(Throwable error) {
            completeExceptionally(error);
        }

        @Override
        public void onCompleted() {
            if (!isDone()) {
                completeExceptionally(new IllegalStateException("publish stream closed without a response"));
            }
        }
    }
}
