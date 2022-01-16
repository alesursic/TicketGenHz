package ticket.gen.hz.event.listeners;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ticket.gen.hz.core.PartitionToRedisSubscribers;
import ticket.gen.hz.redis.LazyRedisSubscriber;

import java.util.Set;

/**
 * When another node joining the cluster completes the bootstrap it connects to redis
 * for partitions it owns and then sends a pubsub message for each partition it formed
 * redis connection. Nodes that used to own these partitions (before the partition migration
 * on new member joining) react on this message and disconnect from redis. Until then
 * there are duplicate connections (temporarily).
 * Member knows which partitions it used to know before migration by remembering them
 * when they are leaving the cluster (on partition migration event).
 */
public class DisconnectOnBootstrap implements MessageListener<Integer> {
    private static final Logger log = LoggerFactory.getLogger(DisconnectOnBootstrap.class);

    private final Set<Integer> lostPartitionIds;
    private final PartitionToRedisSubscribers subscriptionFactories;

    public DisconnectOnBootstrap(
            Set<Integer> lostPartitionIds,
            PartitionToRedisSubscribers subscriptionFactories
    ) {
        this.lostPartitionIds = lostPartitionIds;
        this.subscriptionFactories = subscriptionFactories;
    }

    @Override
    public void onMessage(Message<Integer> message) {
        int partitionId = message.getMessageObject();

        //filter-out only my messages (like with Kafka)
        if (lostPartitionIds.contains(partitionId)) {
            lostPartitionIds.remove(partitionId);
            Set<LazyRedisSubscriber> factories =  subscriptionFactories.getRedisSubscribers(partitionId);
            factories.forEach(redisSubscriber -> {
                log.warn("on-bootstrap disconnecting from {}", redisSubscriber.getHashTag());
                redisSubscriber.interrupt();
            });
        }
    }

    //Helpers

    private static boolean equals(PartitionReplica msgDest, Member localMember) {
        return localMember.getUuid().equals(msgDest.uuid()) &&
                localMember.getAddress().equals(msgDest.address());
    }
}
