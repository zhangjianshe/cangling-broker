package cn.satway.message;

@FunctionalInterface
public interface MessageHandler {
    /**
     * Handle one delivered message. Throw to make the broker retry (non-2xx).
     */
    void onMessage(QueueMessage message) throws Exception;
}
