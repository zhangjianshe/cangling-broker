package cn.mapway.broker;

import java.util.Objects;

public final class TopicConfig {
    public static final String SINGLE = "single";
    public static final String BROADCAST = "broadcast";
    public static final String PERSISTENT = "persistent";
    public static final String EPHEMERAL = "ephemeral";

    private final String topic;
    private final String delivery;
    private final String persistence;

    public TopicConfig(String topic, String delivery) {
        this(topic, delivery, PERSISTENT);
    }

    public TopicConfig(String topic, String delivery, String persistence) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (delivery == null || delivery.isBlank()) {
            throw new IllegalArgumentException("delivery is required");
        }
        this.topic = topic.trim();
        this.delivery = delivery.trim();
        this.persistence = persistence == null || persistence.isBlank()
                ? PERSISTENT
                : persistence.trim();
    }

    public static TopicConfig single(String topic) {
        return new TopicConfig(topic, SINGLE, PERSISTENT);
    }

    public static TopicConfig broadcast(String topic) {
        return new TopicConfig(topic, BROADCAST, PERSISTENT);
    }

    public static TopicConfig ephemeral(String topic, String delivery) {
        return new TopicConfig(topic, delivery, EPHEMERAL);
    }

    public String topic() {
        return topic;
    }

    public String delivery() {
        return delivery;
    }

    public String persistence() {
        return persistence;
    }

    public boolean broadcast() {
        return BROADCAST.equalsIgnoreCase(delivery);
    }

    public boolean ephemeral() {
        return EPHEMERAL.equalsIgnoreCase(persistence);
    }

    @Override
    public String toString() {
        return "TopicConfig{topic='" + topic + "', delivery='" + delivery
                + "', persistence='" + persistence + "'}";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TopicConfig that)) {
            return false;
        }
        return topic.equals(that.topic)
                && delivery.equalsIgnoreCase(that.delivery)
                && persistence.equalsIgnoreCase(that.persistence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, delivery.toLowerCase(), persistence.toLowerCase());
    }
}
