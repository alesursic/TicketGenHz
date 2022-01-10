package ticket.gen.hz;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.PartitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import ticket.gen.hz.core.HzNode;
import ticket.gen.hz.core.PartitionToHashTags;
import ticket.gen.hz.core.PartitionToRedisSubscribers;
import ticket.gen.hz.event.listeners.PartitionMigrationListener;
import ticket.gen.hz.helpers.HashTag;
import ticket.gen.hz.helpers.HashTagToPartition;
import ticket.gen.hz.helpers.ThreadSleep;
import ticket.gen.hz.redis.RedisSubscriberProvider;
import ticket.gen.hz.state.RedisMarketKey;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.*;

public class HzNodeMain {
    private static final Logger log = LoggerFactory.getLogger(HzNodeMain.class);

    public static final List<String> config = Arrays.asList(
            "{develop:bk:0}",
            "{staging:bk:0}",
            "{develop:bk:24505}",
            "{develop:bk:24497}",
            "{staging:bk:24516}",
            "{develop:m:0}",
            "{staging:m:0}",
            "{staging:bk:26653}"
    );

    public static void main(String[] args) {
        Config hzConfig = new Config() ;
        hzConfig.setProperty("hazelcast.shutdownhook.enabled", "false");
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(hzConfig);
        final PartitionService partitionService = hz.getPartitionService();
        final ISet<RedisMarketKey> distributedKeyspace = hz.getSet("distributedKeyspace");
        final IQueue<String> cmds = hz.getQueue("cmds");

        JedisCluster jedisCluster = new JedisCluster(new HostAndPort("localhost", 7000));
        final RedisSubscriberProvider redisSubscriberProvider = new RedisSubscriberProvider(distributedKeyspace, jedisCluster);

        final HashTagToPartition toPartition = new HashTagToPartition(partitionService::getPartition);
        final PartitionToHashTags partitionToHashTags = new PartitionToHashTags(config
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
        //********************************************Redis*****************************************
        //NOTE: Has partition migration completed by this point?
        //creates and starts redis threads
        allRedisSubscribers
                .streamLocal()
                .forEach(entry -> entry
                                .getValue()
                                .stream()
                                .forEach(redisSubscriber -> {
                                    System.out.println("on-bootstrap connecting to Redis " + redisSubscriber.getHashTag());
                                    Thread t = new Thread(redisSubscriber);
                                    t.start();
                                })
                );

        partitionService.addMigrationListener(new PartitionMigrationListener(allRedisSubscribers));
        //******************************************************************************************

        HzNode hzNode = new HzNode(partitionService, cmds, distributedKeyspace, partitionToHashTags);
        Thread t = new Thread(hzNode, "hz-node-thread");
        t.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Starting shutdown hook");
            t.interrupt();
            System.out.println("Interrupted current thread");

            //**************************************************
            Set<Integer> ownedPartitionIds = partitionToHashTags.ownedPartitionIds();
            System.out.println("owner partition-ids");
            System.out.println(ownedPartitionIds);
            //**************************************************

            ThreadSleep.sleep(1000);
            log.warn("Shutting down hz node..");
            hz.shutdown();
        }));
    }
}
