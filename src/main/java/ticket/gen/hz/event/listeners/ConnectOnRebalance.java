package ticket.gen.hz.event.listeners;

import com.hazelcast.cluster.Member;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ticket.gen.hz.core.PartitionToRedisSubscribers;
import ticket.gen.hz.redis.LazyRedisSubscriber;

import java.util.Set;

import static ticket.gen.hz.helpers.ThreadUtil.subscribeToRedis;

/**
 * When another node is leaving the cluster (e.g. graceful shutdown on sigint)
 * its partitions are migrated to other nodes of the cluster. When a partition is
 * migrated the callback in this class is invoked. It connects to the same Redis node
 * as the previous member - temporarily there are duplicate connections.
 * Node leaving the cluster waits for other members to finish the connections and then
 * it terminates which also closes all of its redis connections (no more duplicate
 * connections).
 */
public class ConnectOnRebalance implements MigrationListener {
    private static final Logger log = LoggerFactory.getLogger(ConnectOnRebalance.class);

    private final PartitionToRedisSubscribers partitionToRedisSubscribers;
    private final Set<Integer> lostPartitionIds;

    //based on hash-tags in config server => for each hash-tah: hash-tag -> hash-slot -> partition-id

    public ConnectOnRebalance(
            PartitionToRedisSubscribers partitionToRedisSubscribers,
            Set<Integer> lostPartitionIds
    ) {
        this.partitionToRedisSubscribers = partitionToRedisSubscribers;
        this.lostPartitionIds = lostPartitionIds;
    }

    /*
     * Connect to Redis taking over connections of a source hazelcast node.
     * Temporarily there will be double connections until migration is completed and the source
     * node drops the secondary connection
     */
    @Override
    public void migrationStarted(MigrationState state) {
    }

    @Override
    public void migrationFinished(MigrationState state) {
    }

    /*
     * Disconnects from Redis when partition is completely lost because
     * it has been transferred to a new node which is took over redis connections
     * since migration started
     */
    @Override
    public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
        int partitionId = event.getPartitionId();
        //primary (master) partition was migrated
        if (event.getReplicaIndex() == 0 && partitionToRedisSubscribers.containsPartition(partitionId)) {
            Member src = event.getSource();
            Member dest = event.getDestination();
            boolean loosingPartition = src != null && src.localMember();
            boolean gainingPartition = dest != null && dest.localMember();

            if (gainingPartition) {
                for (LazyRedisSubscriber lazyRedisSubscriber : partitionToRedisSubscribers.getRedisSubscribers(partitionId)) {
                    //only connect if not already connected (happens on a sigkill of other node)
                    subscribeToRedis(log, lazyRedisSubscriber, "on-migration");
                }
            } else if (loosingPartition) {
                lostPartitionIds.add(partitionId);
            }
        }
    }

    @Override
    public void replicaMigrationFailed(ReplicaMigrationEvent event) {

    }
}
