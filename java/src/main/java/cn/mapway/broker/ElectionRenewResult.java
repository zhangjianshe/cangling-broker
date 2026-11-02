package cn.mapway.broker;

/** Result of renewing an exact leader/term/lease tuple. */
public final class ElectionRenewResult {
    private final boolean renewed;
    private final ElectionLeader leader;

    public ElectionRenewResult(boolean renewed, ElectionLeader leader) {
        this.renewed = renewed;
        this.leader = leader;
    }

    public boolean renewed() { return renewed; }
    public ElectionLeader leader() { return leader; }
}
