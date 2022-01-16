package ticket.gen.hz;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.topic.ITopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import ticket.gen.hz.core.HazelcastInstanceFactory;
import ticket.gen.hz.core.HzQueueHandler;
import ticket.gen.hz.core.PartitionToHashTags;
import ticket.gen.hz.core.PartitionToRedisSubscribers;
import ticket.gen.hz.event.listeners.ConnectOnRebalance;
import ticket.gen.hz.event.listeners.DisconnectOnBootstrap;
import ticket.gen.hz.helpers.HashTagToPartitionF;
import ticket.gen.hz.helpers.ThreadUtil;
import ticket.gen.hz.redis.HashTag;
import ticket.gen.hz.redis.LazyRedisSubscriber;
import ticket.gen.hz.redis.RedisSubscriberProvider;
import ticket.gen.hz.state.RedisMarketKey;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.*;
import static ticket.gen.hz.helpers.ThreadUtil.subscribeToRedis;

public class HzNodeMain {
    private static final Logger log = LoggerFactory.getLogger(HzNodeMain.class);

    public static void main(String[] args) {
        log.info("---------------------------------------hz-node-main------------------------------------");
        HazelcastInstance hz = HazelcastInstanceFactory.create();
        final PartitionService partitionService = hz.getPartitionService();
        final ISet<RedisMarketKey> distributedKeyspace = hz.getSet("distributedKeyspace");
        final IQueue<String> cmds = hz.getQueue("cmds");

        final JedisCluster jedisCluster = new JedisCluster(new HostAndPort("localhost", 7000));
        final RedisSubscriberProvider redisSubscriberProvider = new RedisSubscriberProvider(distributedKeyspace, jedisCluster);
        final HashTagToPartitionF toPartition = new HashTagToPartitionF(partitionService::getPartition);
        final PartitionToHashTags partitionToHashTags = new PartitionToHashTags(HazelcastInstanceFactory
                .config
                .stream()
                .map(HashTag::fromString)
                .collect(groupingBy(toPartition, toSet()))
        );
        final PartitionToRedisSubscribers allRedisSubscribers = new PartitionToRedisSubscribers(partitionToHashTags
                .stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> entry
                                .getValue()
                                .stream()
                                .map(redisSubscriberProvider)
                                .collect(toSet())
                ))
        );

        //Event listeners:
        Set<Integer> lostPartitionIds = ConcurrentHashMap.newKeySet();
        partitionService.addMigrationListener(
                new ConnectOnRebalance(allRedisSubscribers, lostPartitionIds)
        );

        ITopic<Integer> disconnectOnBootstrapTopic = hz.getTopic("disconnect-on-bootstrap");
        disconnectOnBootstrapTopic.addMessageListener(new DisconnectOnBootstrap(lostPartitionIds, allRedisSubscribers));
        allRedisSubscribers
                .streamLocal()
                .forEach(entry -> {
                            Partition partition = entry.getKey();
                            Set<LazyRedisSubscriber> redisSubscribers = entry.getValue();

                            redisSubscribers.forEach(redisSubscriber -> {
                                subscribeToRedis(log, redisSubscriber, "on-bootstrap");
                                ThreadUtil.sleep(250); //duplicate connections for a short time
                                disconnectOnBootstrapTopic.publish(partition.getPartitionId());
                            });
                        }
                );
        //.

        Thread hzNodeThread = new Thread(
                new HzQueueHandler(partitionService, cmds, distributedKeyspace, partitionToHashTags),
                "hz-node-thread"
        );
        hzNodeThread.start();

        Runtime
                .getRuntime()
                .addShutdownHook(new Thread(() -> {
                    /*
                     * Shutdown does not disconnect from redis on the member leaving the cluster
                     * while connections are formed in members receiving these partitions
                     * Sleep has to be long enough so that double connections are kept until receiving members
                     * successfully reconnect.
                     */
                    hz.shutdown();
                    ThreadUtil.sleep(500); //of course an ack is a safer approach than sleep
                }));
    }
}
