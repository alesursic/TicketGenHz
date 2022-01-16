package ticket.gen.hz.core;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.partition.PartitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ticket.gen.hz.helpers.PartitionsStats;
import ticket.gen.hz.state.RedisMarketKey;

import java.util.concurrent.TimeUnit;

import static ticket.gen.hz.helpers.PartitionsStats.fromKeys;

public final class HzQueueHandler implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(HzQueueHandler.class);

    private final PartitionService partitionService;
    private final IQueue<String> cmdQueue; //input (RMQ simulation)
    private final ISet<RedisMarketKey> distributedKeyspace; //output (distributed cache)
    private final PartitionToHashTags partitionToHashTags;

    public HzQueueHandler(
            PartitionService partitionService,
            IQueue<String> cmdQueue,
            ISet<RedisMarketKey> distributedKeyspace,
            PartitionToHashTags partitionToHashTags
    ) {
        this.partitionService = partitionService;
        this.cmdQueue = cmdQueue;
        this.distributedKeyspace = distributedKeyspace;
        this.partitionToHashTags = partitionToHashTags;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                //why does cmdQueue.take not support thread interrupts?
                String cmd = cmdQueue.poll(100, TimeUnit.MILLISECONDS);
                if (cmd == null) {
                    continue;
                }

                log.debug("Consumed {}", cmd);
                if (cmd.equals("keys")) { //display current status
                    PartitionsStats stats = fromKeys(distributedKeyspace, partitionService::getPartition);
                    log.info("Total number of keys {}", stats.totalCount());
                    log.info("{}", stats);
                } else if (cmd.equals("partitions")) { //display current status
                    partitionToHashTags.stream().forEach(partitionHashTags -> {
                        log.info("{}", partitionHashTags.getKey());
                        log.info("{}", partitionHashTags.getValue());
                    });
                } else {
                    //handle new dirty key (keyspace notification frorm Redis) [debugging-only]
                    String changedRedisKey = cmd;
                    RedisMarketKey parsed = RedisMarketKey.parse(changedRedisKey);
                    distributedKeyspace.add(parsed);
                }
            } catch (InterruptedException e) {
                log.warn("HzNode's event loop has been interrupted. Exiting..");
                Thread.currentThread().interrupt();
            }
        }
        log.info("Consumer Finished!");
    }
}
