package cn.mapway.broker;

@FunctionalInterface
public interface MessageHandler {
    /**
     * Handle one delivered message. Throw to make the broker retry (non-2xx).
     */
    void onMessage(SatwayMessage message) throws Exception;
}
