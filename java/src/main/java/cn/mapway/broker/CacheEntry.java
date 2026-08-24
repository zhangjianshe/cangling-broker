package cn.mapway.broker;

/**
 * A cached value plus its type hint, returned by {@link SatwayClient#cacheGetEntry(String)}.
 */
public final class CacheEntry {
    private final byte[] value;
    private final String valueType;

    public CacheEntry(byte[] value, String valueType) {
        this.value = value == null ? new byte[0] : value;
        this.valueType = valueType == null ? "string" : valueType;
    }

    public byte[] value() {
        return value;
    }

    /** One of {@code string} / {@code long} / {@code int} / {@code double} / {@code bool}. */
    public String valueType() {
        return valueType;
    }
}
