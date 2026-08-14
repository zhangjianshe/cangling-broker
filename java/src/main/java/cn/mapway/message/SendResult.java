package cn.mapway.message;

public final class SendResult {
    private final String messageId;
    private final boolean duplicate;

    public SendResult(String messageId, boolean duplicate) {
        this.messageId = messageId;
        this.duplicate = duplicate;
    }

    public String messageId() {
        return messageId;
    }

    public boolean duplicate() {
        return duplicate;
    }

    @Override
    public String toString() {
        return "SendResult{messageId='" + messageId + "', duplicate=" + duplicate + "}";
    }
}
