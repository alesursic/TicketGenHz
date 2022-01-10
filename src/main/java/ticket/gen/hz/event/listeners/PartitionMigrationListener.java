package ticket.gen.hz.event.listeners;

import com.hazelcast.cluster.Member;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ticket.gen.hz.core.PartitionToRedisSubscribers;
import ticket.gen.hz.redis.LazyRedisSubscriber;

public class PartitionMigrationListener implements MigrationListener {
    private static final Logger log = LoggerFactory.getLogger(PartitionMigrationListener.class);

    private final PartitionToRedisSubscribers partitionToRedisSubscribers;

    //based on hash-tags in config server => for each hash-tah: hash-tag -> hash-slot -> partition-id
    public PartitionMigrationListener(PartitionToRedisSubscribers partitionToRedisSubscribers) {
        this.partitionToRedisSubscribers = partitionToRedisSubscribers;
    }

    @Override
    public void migrationStarted(MigrationState state) {

    }

    @Override
    public void migrationFinished(MigrationState state) {

    }

    @Override
    public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
        log.info("replica migration completed");

        int partitionId = event.getPartitionId();
        //primary (master) partition was migrated
        if (event.getReplicaIndex() == 0 && partitionToRedisSubscribers.containsPartition(partitionId)) {
            Member src = event.getSource();
            Member dest = event.getDestination();

            boolean loosingPartition = src != null && src.localMember();
            boolean gainingPartition = dest != null && dest.localMember();
            if (loosingPartition || gainingPartition) {
                for (LazyRedisSubscriber lazyRedisSubscriber : partitionToRedisSubscribers.getRedisSubscribers(partitionId)) {
                    if (loosingPartition) {
                        lazyRedisSubscriber.interrupt();
                        System.out.printf("on-migration disconnecting from %s..\n", lazyRedisSubscriber.getHashTag());
                    } else {
                        Thread t = new Thread(lazyRedisSubscriber);
                        t.start();
                        System.out.printf("on-migration connecting to %s..\n", lazyRedisSubscriber.getHashTag());
                    }
                }
            }

            //todo: disconnect if temporarily owned a redundant partition's redis connection
        }
    }

    @Override
    public void replicaMigrationFailed(ReplicaMigrationEvent event) {

    }
}
