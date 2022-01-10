package ticket.gen.hz.core;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.partition.PartitionService;
import ticket.gen.hz.helpers.PartitionsStats;
import ticket.gen.hz.state.RedisMarketKey;

import java.util.concurrent.TimeUnit;

import static ticket.gen.hz.helpers.PartitionsStats.fromKeys;

public final class HzNode implements Runnable {
    private final PartitionService partitionService;
    private final IQueue<String> cmdQueue; //input (RMQ simulation)
    private final ISet<RedisMarketKey> distributedKeyspace; //output (distributed cache)
    private final PartitionToHashTags partitionToHashTags;

    public HzNode(
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

                System.out.println("Consumed: " + cmd);
                if (cmd.equals("keys")) { //display current status
                    PartitionsStats stats = fromKeys(distributedKeyspace, partitionService::getPartition);
                    System.out.println("Total number of keys " + stats.totalCount());
                    System.out.println(stats);
                } else if (cmd.equals("partitions")) { //display current status
                    partitionToHashTags.stream().forEach(partitionHashTags -> {
                        System.out.println(partitionHashTags.getKey());
                        System.out.println(partitionHashTags.getValue());
                    });
                } else if (cmd.startsWith("terminating")) {
                    int partitionId = Integer.parseInt(cmd.split(" ")[1]);
                    //todo: subscribe to redis node corresponding to hash-slot that corresponds to this partition-id
                    System.out.println();
                } else {
                    //handle new dirty key (keyspace notification frorm Redis)
                    String changedRedisKey = cmd;
                    RedisMarketKey parsed = RedisMarketKey.parse(changedRedisKey);
                    distributedKeyspace.add(parsed);
                }
            } catch (InterruptedException e) {
                System.out.println("HzNode's event loop has been interrupted. Exiting..");
                break;
            }
        }
        System.out.println("Consumer Finished!");
    }
}
