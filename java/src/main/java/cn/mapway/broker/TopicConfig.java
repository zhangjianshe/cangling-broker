package cn.mapway.broker;

import java.util.Objects;

public final class TopicConfig {
    public static final String SINGLE = "single";
    public static final String BROADCAST = "broadcast";

    private final String topic;
    private final String delivery;

    public TopicConfig(String topic, String delivery) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (delivery == null || delivery.isBlank()) {
            throw new IllegalArgumentException("delivery is required");
        }
        this.topic = topic.trim();
        this.delivery = delivery.trim();
    }

    public static TopicConfig single(String topic) {
        return new TopicConfig(topic, SINGLE);
    }

    public static TopicConfig broadcast(String topic) {
        return new TopicConfig(topic, BROADCAST);
    }

    public String topic() {
        return topic;
    }

    public String delivery() {
        return delivery;
    }

    public boolean broadcast() {
        return BROADCAST.equalsIgnoreCase(delivery);
    }

    @Override
    public String toString() {
        return "TopicConfig{topic='" + topic + "', delivery='" + delivery + "'}";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TopicConfig that)) {
            return false;
        }
        return topic.equals(that.topic) && delivery.equalsIgnoreCase(that.delivery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, delivery.toLowerCase());
    }
}
