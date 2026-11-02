package cn.mapway.broker;

/** Current observed state of a named election. */
public final class ElectionState {
    private final boolean found;
    private final boolean active;
    private final ElectionLeader leader;

    public ElectionState(boolean found, boolean active, ElectionLeader leader) {
        this.found = found;
        this.active = active;
        this.leader = leader;
    }

    public boolean found() { return found; }
    public boolean active() { return active; }
    public ElectionLeader leader() { return leader; }
}
