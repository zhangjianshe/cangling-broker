package cn.mapway.message;

import java.time.Duration;
import java.util.Objects;

public final class SubscribeOptions {
    private final String topic;
    private final String listenHost;
    private final int listenPort;
    private final String callbackUrl;
    private final Duration heartbeat;

    private SubscribeOptions(Builder builder) {
        this.topic = builder.topic;
        this.listenHost = builder.listenHost;
        this.listenPort = builder.listenPort;
        this.callbackUrl = builder.callbackUrl;
        this.heartbeat = builder.heartbeat;
    }

    public static Builder topic(String topic) {
        return new Builder(topic);
    }

    public String topic() {
        return topic;
    }

    public String listenHost() {
        return listenHost;
    }

    public int listenPort() {
        return listenPort;
    }

    public String callbackUrl() {
        if (callbackUrl != null && !callbackUrl.isBlank()) {
            return callbackUrl;
        }
        if ("0.0.0.0".equals(listenHost) || "::".equals(listenHost)) {
            throw new IllegalArgumentException("callbackUrl is required when listenHost is " + listenHost);
        }
        return "http://" + listenHost + ":" + listenPort + "/messages";
    }

    public Duration heartbeat() {
        return heartbeat;
    }

    public static final class Builder {
        private final String topic;
        private String listenHost = "127.0.0.1";
        private int listenPort = 8080;
        private String callbackUrl;
        private Duration heartbeat = Duration.ofSeconds(15);

        private Builder(String topic) {
            this.topic = Objects.requireNonNull(topic, "topic");
            if (topic.isBlank()) {
                throw new IllegalArgumentException("topic is required");
            }
        }

        public Builder listen(String host, int port) {
            this.listenHost = Objects.requireNonNull(host, "host");
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("invalid listen port: " + port);
            }
            this.listenPort = port;
            return this;
        }

        public Builder callbackUrl(String callbackUrl) {
            this.callbackUrl = callbackUrl;
            return this;
        }

        public Builder heartbeat(Duration heartbeat) {
            this.heartbeat = heartbeat == null || heartbeat.isZero() || heartbeat.isNegative()
                    ? Duration.ofSeconds(15)
                    : heartbeat;
            return this;
        }

        public SubscribeOptions build() {
            return new SubscribeOptions(this);
        }
    }
}
