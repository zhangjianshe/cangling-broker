package cn.mapway.broker;

/**
 * A held distributed lock returned by {@link SatwayClient#acquireLock(String, long)}.
 *
 * <p>Renew the lease with {@link #renew(long)} and release it with {@link #release()}.
 * Releasing only succeeds while this handle's {@code owner} token still holds the key,
 * so an expired or stolen lock cannot be released by an old holder.</p>
 */
public final class LockHandle implements AutoCloseable {
    private final SatwayClient client;
    private final String lockKey;
    private final String owner;

    LockHandle(SatwayClient client, String lockKey, String owner) {
        this.client = client;
        this.lockKey = lockKey;
        this.owner = owner;
    }

    /** The lock key this handle acquired. */
    public String lockKey() {
        return lockKey;
    }

    /** The random owner token that holds the lock. */
    public String owner() {
        return owner;
    }

    /** Extend the lease. Returns false when the lock was lost or already released. */
    public boolean renew(long ttlSeconds) {
        if (ttlSeconds <= 0) {
            throw new IllegalArgumentException("ttlSeconds must be > 0");
        }
        return client.lockRenew(lockKey, owner, ttlSeconds);
    }

    /** Release the lock if this handle still owns it. */
    public boolean release() {
        return client.lockRelease(lockKey, owner);
    }

    @Override
    public void close() {
        release();
    }
}
