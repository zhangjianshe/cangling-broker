package cn.mapway.message;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class SubscribeOptions {
    private final String topic;
    private final String consumerId;
    private final String name;
    private final Map<String, String> attributes;

    private SubscribeOptions(Builder builder) {
        this.topic = builder.topic;
        this.consumerId = builder.consumerId;
        this.name = builder.name;
        this.attributes = builder.attributes;
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

    public static final class Builder {
        private final String topic;
        private String consumerId = "";
        private String name = "";
        private Map<String, String> attributes = Map.of();

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

        public SubscribeOptions build() {
            return new SubscribeOptions(this);
        }
    }
}
