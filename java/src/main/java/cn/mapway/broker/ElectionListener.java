package cn.mapway.broker;

/** Leadership transition callbacks for {@link ElectionHandle}. */
public interface ElectionListener {
    default void onLeader(ElectionHandle handle, ElectionLeader leader) {}
    default void onFollower(ElectionHandle handle, ElectionLeader observedLeader) {}
}
