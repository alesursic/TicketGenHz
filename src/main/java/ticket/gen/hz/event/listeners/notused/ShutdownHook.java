package ticket.gen.hz.event.listeners.notused;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionStateGenerator;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionStateGeneratorImpl;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.topic.ITopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ticket.gen.hz.core.PartitionToHashTags;
import ticket.gen.hz.event.data.PartitionHandoverEvent;
import ticket.gen.hz.helpers.ThreadUtil;

import java.util.Collection;
import java.util.Set;

public class ShutdownHook implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ShutdownHook.class);

    private final HazelcastInstance hz;
    private final Thread hzNodeThread;
    private final PartitionToHashTags partitionToHashTags;
    private final ITopic<PartitionHandoverEvent> handoverTopic;

    public ShutdownHook(
            HazelcastInstance hz,
            Thread hzNodeThread,
            PartitionToHashTags partitionToHashTags,
            ITopic<PartitionHandoverEvent> handoverTopic
    ) {
        this.hz = hz;
        this.hzNodeThread = hzNodeThread;
        this.partitionToHashTags = partitionToHashTags;
        this.handoverTopic = handoverTopic;
    }

    @Override
    public void run() {
        HazelcastInstanceImpl hzImpl = ((HazelcastInstanceProxy)hz).getOriginal();
        InternalPartitionServiceImpl internalPartitionService = (InternalPartitionServiceImpl) hzImpl.node.getPartitionService();

        log.warn("Starting shutdown hook");
        hzNodeThread.interrupt();
        log.warn("Interrupted current thread");

        //**************************************************************************************
        Set<Integer> ownedPartitionIds = partitionToHashTags.ownedPartitionIds();
        log.info("owner partition-ids {}", ownedPartitionIds);

        InternalPartition[] currentState = internalPartitionService.getInternalPartitions();
        PartitionStateGenerator consistentHashing = new PartitionStateGeneratorImpl();
        PartitionStateManager stateMgr = internalPartitionService.getPartitionStateManager();
        Collection<MemberGroup> memberGroups = stateMgr.createMemberGroups(
                ImmutableSet.of(hz.getCluster().getLocalMember())
        );

        PartitionReplica[][] newState = consistentHashing.arrange(memberGroups, currentState);

        //iterate over newState
//        for (int localPartitionId : ownedPartitionIds) {
//            PartitionReplica newReplicaOwner = newState[localPartitionId][0];
//            PartitionHandoverEvent msg = new PartitionHandoverEvent(newReplicaOwner, localPartitionId);
//            handoverTopic.publish(msg);
//            log.debug("Sent: {}", msg);
//        }
        //**************************************************************************************


        log.warn("Shutting down hz node..");
        ThreadUtil.sleep(500);
        hz.shutdown();
        ThreadUtil.sleep(500);
        log.debug("Shutdown hook completed");
    }
}
