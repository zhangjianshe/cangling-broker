package cn.mapway.message;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class QueueMessage {
    private String id;
    private String topic;
    private String payload;

    @JsonProperty("payload_encoding")
    private String payloadEncoding;

    private Map<String, String> attributes;

    @JsonProperty("created_at")
    private String createdAt;

    public String id() {
        return id;
    }

    public String topic() {
        return topic;
    }

    public String payload() {
        return payload;
    }

    public String payloadEncoding() {
        return payloadEncoding;
    }

    public Map<String, String> attributes() {
        return attributes == null ? Collections.emptyMap() : attributes;
    }

    public String createdAt() {
        return createdAt;
    }

    public byte[] payloadBytes() {
        if (payload == null) {
            return new byte[0];
        }
        if ("base64".equalsIgnoreCase(payloadEncoding)) {
            return Base64.getDecoder().decode(payload);
        }
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public void setPayloadEncoding(String payloadEncoding) {
        this.payloadEncoding = payloadEncoding;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "QueueMessage{id='" + id + "', topic='" + topic + "', payload='" + payload + "'}";
    }
}
