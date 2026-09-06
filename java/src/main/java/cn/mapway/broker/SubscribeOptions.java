package cn.mapway.broker;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class SubscribeOptions {
    private final String topic;
    private final String consumerId;
    private final String name;
    private final Map<String, String> attributes;
    private final int concurrency;

    private SubscribeOptions(Builder builder) {
        this.topic = builder.topic;
        this.consumerId = builder.consumerId;
        this.name = builder.name;
        this.attributes = builder.attributes;
        this.concurrency = builder.concurrency;
    }

    public static Builder topic(String topic) {
        return new Builder(topic);
    }

    public String topic() {
        return topic;
    }

    public String consumerId() {
        return consumerId;
    }

    public String name() {
        return name;
    }

    public Map<String, String> attributes() {
        return attributes;
    }

    /**
     * Number of parallel subscription streams used to process this topic.
     * Keep this at {@code 1} for broadcast topics because every stream receives
     * a copy. Values greater than one are intended for competing-consumer
     * ({@code single}) topics. The same message handler is invoked concurrently
     * by these workers and therefore must be thread-safe.
     */
    public int concurrency() {
        return concurrency;
    }

    public static final class Builder {
        private final String topic;
        private String consumerId = "";
        private String name = "";
        private Map<String, String> attributes = Map.of();
        private int concurrency = 1;

        private Builder(String topic) {
            this.topic = Objects.requireNonNull(topic, "topic");
            if (topic.isBlank()) {
                throw new IllegalArgumentException("topic is required");
            }
        }

        public Builder consumerId(String consumerId) {
            this.consumerId = consumerId == null ? "" : consumerId;
            return this;
        }

        public Builder name(String name) {
            this.name = name == null ? "" : name;
            return this;
        }

        public Builder attributes(Map<String, String> attributes) {
            this.attributes = attributes == null ? Map.of() : Collections.unmodifiableMap(attributes);
            return this;
        }

        /**
         * Use this many parallel subscription workers. This should only be
         * greater than one for topics configured with {@code delivery=single}.
         * The message handler must be safe for concurrent calls.
         */
        public Builder concurrency(int concurrency) {
            if (concurrency < 1) {
                throw new IllegalArgumentException("concurrency must be at least 1");
            }
            this.concurrency = concurrency;
            return this;
        }

        public SubscribeOptions build() {
            return new SubscribeOptions(this);
        }
    }
}
