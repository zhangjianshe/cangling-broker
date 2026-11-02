package cn.mapway.broker;

/** Immutable leader lease returned by the broker election service. */
public final class ElectionLeader {
    private final String election;
    private final String leaderId;
    private final long term;
    private final String leaseId;
    private final String expireAt;
    private final long remainingTtlMillis;

    public ElectionLeader(String election, String leaderId, long term, String leaseId,
                          String expireAt, long remainingTtlMillis) {
        this.election = election;
        this.leaderId = leaderId;
        this.term = term;
        this.leaseId = leaseId;
        this.expireAt = expireAt;
        this.remainingTtlMillis = remainingTtlMillis;
    }

    public String election() { return election; }
    public String leaderId() { return leaderId; }
    public long term() { return term; }
    public String leaseId() { return leaseId; }
    public String expireAt() { return expireAt; }
    public long remainingTtlMillis() { return remainingTtlMillis; }
}
