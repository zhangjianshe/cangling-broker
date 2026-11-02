package cn.mapway.broker;

import io.grpc.Context;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Continuously campaigns and renews one named election.
 *
 * <p>{@link #isLeader()} also checks a monotonic local safety deadline. A
 * disconnected client therefore fails closed even while its renewal thread is
 * waiting for an RPC deadline.</p>
 */
public final class ElectionHandle implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(ElectionHandle.class.getName());

    private final SatwayClient client;
    private final String election;
    private final String candidateId;
    private final long ttlSeconds;
    private final ElectionListener listener;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Context.CancellableContext> activeCall = new AtomicReference<>();
    private final Thread worker;
    private volatile ElectionLeader observedLeader;
    private volatile boolean leading;
    private volatile long validUntilNanos;

    ElectionHandle(SatwayClient client, String election, String candidateId,
                   long ttlSeconds, ElectionListener listener) {
        this.client = client;
        this.election = election;
        this.candidateId = candidateId;
        this.ttlSeconds = ttlSeconds;
        this.listener = listener == null ? new ElectionListener() {} : listener;
        this.worker = new Thread(this::run, "cangling-election-" + election.replace('/', '-'));
        this.worker.setDaemon(true);
    }

    void start() { worker.start(); }

    public String election() { return election; }
    public String candidateId() { return candidateId; }
    public ElectionLeader leader() { return observedLeader; }
    public long term() { return isLeader() && observedLeader != null ? observedLeader.term() : 0; }

    public boolean isLeader() {
        return leading && System.nanoTime() < validUntilNanos;
    }

    private void run() {
        long refreshMillis = Math.max(1_000, TimeUnit.SECONDS.toMillis(ttlSeconds) / 3);
        while (running.get() && client.isOpen()) {
            try {
                if (isLeader() && observedLeader != null) {
                    ElectionRenewResult result = call(() -> client.renewElectionOnce(
                            election, candidateId, observedLeader.term(),
                            observedLeader.leaseId(), ttlSeconds));
                    if (!running.get()) {
                        break;
                    }
                    if (result.renewed() && result.leader() != null) {
                        becomeLeader(result.leader());
                    } else {
                        becomeFollower(result.leader());
                    }
                } else {
                    ElectionCampaignResult result = call(() -> client.campaignElectionOnce(
                            election, candidateId, ttlSeconds));
                    if (!running.get()) {
                        if (result.won()) {
                            resignQuietly(result.leader());
                        }
                        break;
                    }
                    if (result.won()) {
                        becomeLeader(result.leader());
                    } else {
                        becomeFollower(result.leader());
                    }
                }
                sleep(refreshMillis);
            } catch (RuntimeException error) {
                if (!running.get()) {
                    break;
                }
                if (!isLeader()) {
                    becomeFollower(observedLeader);
                }
                LOG.log(Level.WARNING, "election request failed: " + election, error);
                sleep(Math.min(1_000, refreshMillis));
            }
        }
        becomeFollower(observedLeader);
    }

    private <T> T call(Callable<T> operation) {
        Context.CancellableContext context = Context.current().withCancellation();
        activeCall.set(context);
        if (!running.get()) {
            context.cancel(null);
        }
        try {
            return context.call(operation);
        } catch (RuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("election operation failed", error);
        } finally {
            activeCall.compareAndSet(context, null);
            context.close();
        }
    }

    private void becomeLeader(ElectionLeader leader) {
        long safetyMillis = Math.min(1_000, TimeUnit.SECONDS.toMillis(ttlSeconds) / 3);
        long validForMillis = leader.remainingTtlMillis() - safetyMillis;
        if (validForMillis <= 0) {
            becomeFollower(leader);
            return;
        }
        boolean transition = !isLeader();
        observedLeader = leader;
        validUntilNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(validForMillis);
        leading = true;
        if (transition) {
            try {
                listener.onLeader(this, leader);
            } catch (RuntimeException error) {
                LOG.log(Level.WARNING, "onLeader callback failed: " + election, error);
            }
        }
    }

    private void becomeFollower(ElectionLeader leader) {
        boolean transition = leading;
        observedLeader = leader;
        leading = false;
        validUntilNanos = 0;
        if (transition) {
            try {
                listener.onFollower(this, leader);
            } catch (RuntimeException error) {
                LOG.log(Level.WARNING, "onFollower callback failed: " + election, error);
            }
        }
    }

    private void resignQuietly(ElectionLeader leader) {
        if (leader == null) {
            return;
        }
        try {
            client.resignElectionOnce(election, candidateId, leader.term(), leader.leaseId());
        } catch (RuntimeException error) {
            LOG.log(Level.FINE, "could not resign election " + election, error);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            if (running.get()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Context.CancellableContext call = activeCall.getAndSet(null);
        if (call != null) {
            call.cancel(null);
        }
        worker.interrupt();
        ElectionLeader leader = observedLeader;
        if (isLeader() && leader != null) {
            resignQuietly(leader);
        }
        becomeFollower(leader);
        client.removeElection(this);
    }
}
