package ticket.gen.hz.event.listeners.notused;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ticket.gen.hz.core.PartitionToRedisSubscribers;
import ticket.gen.hz.event.data.PartitionHandoverEvent;
import ticket.gen.hz.redis.LazyRedisSubscriber;

import java.util.Set;

import static ticket.gen.hz.helpers.ThreadUtil.subscribeToRedis;

public class PartitionHandoverListener implements MessageListener<PartitionHandoverEvent> {
    private static final Logger log = LoggerFactory.getLogger(PartitionHandoverListener.class);

    private final Member localMember;
    private final Set<Integer> temporaryPartitionIds;
    private final PartitionToRedisSubscribers subscriptionFactories;

    public PartitionHandoverListener(
            Member localMember,
            Set<Integer> temporaryPartitionIds,
            PartitionToRedisSubscribers subscriptionFactories
    ) {
        this.localMember = localMember;
        this.temporaryPartitionIds = temporaryPartitionIds;
        this.subscriptionFactories = subscriptionFactories;
    }

    @Override
    public void onMessage(Message<PartitionHandoverEvent> message) {
        PartitionHandoverEvent body = message.getMessageObject();

        //filter-out only my messages (like with Kafka)
        if (equals(body.getDest(), localMember)) {
            int partitionId = body.getPartitionId();
            Set<LazyRedisSubscriber> factories =  subscriptionFactories.getRedisSubscribers(partitionId);

            factories.forEach(redisSubscriber -> subscribeToRedis(log, redisSubscriber, "on-handover"));
            temporaryPartitionIds.add(partitionId);
        }
    }

    //Helpers

    private static boolean equals(PartitionReplica msgDest, Member localMember) {
        return localMember.getUuid().equals(msgDest.uuid()) &&
                localMember.getAddress().equals(msgDest.address());
    }
}
