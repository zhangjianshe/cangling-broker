package cn.mapway.broker;

/**
 * Notified when the gRPC channel reaches {@code READY}.
 * Fired on the first connect and again after every reconnect.
 */
@FunctionalInterface
public interface ConnectionListener {
    void onConnected(SatwayClient client);

    default void onDisconnected(SatwayClient client) {}
}
