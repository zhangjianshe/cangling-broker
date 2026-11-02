package cn.mapway.broker;

/** Result of one election campaign attempt. */
public final class ElectionCampaignResult {
    private final boolean won;
    private final ElectionLeader leader;

    public ElectionCampaignResult(boolean won, ElectionLeader leader) {
        this.won = won;
        this.leader = leader;
    }

    public boolean won() { return won; }
    public ElectionLeader leader() { return leader; }
}
