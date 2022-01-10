package ticket.gen.hz.helpers;

import com.hazelcast.partition.Partition;
import ticket.gen.hz.state.RedisMarketKey;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

public class PartitionsStats {
    private final long totalCount;
    private final Map<Partition, HashSlotsStats> partitionHashSlot;

    public PartitionsStats(Map<Partition, HashSlotsStats> partitionHashSlot) {
        this.partitionHashSlot = partitionHashSlot;
        totalCount = partitionHashSlot
                .values()
                .stream()
                .flatMap(hashSlotsStats -> hashSlotsStats.hashSlotKeyCount.values().stream())
                .reduce(0L, Long::sum);
    }

    public Set<Partition> getPartitions() {
        return partitionHashSlot.keySet();
    }

    public long totalCount() {
        return totalCount;
    }

    public Set<Integer> getPartitionIds() {
        return getPartitions()
                .stream()
                .map(Partition::getPartitionId)
                .collect(toSet());
    }

    @Override
    public String toString() {
        return partitionHashSlot
                .entrySet()
                .stream()
                .map(entry -> String.format(
                        "%s\n%s",
                        entry.getKey(),
                        entry.getValue()
                ))
                .collect(joining("\n"));
    }

    private static class HashSlotsStats {
        private final Map<Integer, Long> hashSlotKeyCount;

        public HashSlotsStats(Map<Integer, Long> hashSlotKeyCount) {
            this.hashSlotKeyCount = hashSlotKeyCount;
        }

        @Override
        public String toString() {
            return hashSlotKeyCount
                    .entrySet()
                    .stream()
                    .map(entry -> String.format(
                            "hash-slot: %d, count(keys): %d",
                            entry.getKey(),
                            entry.getValue()
                    ))
                    .collect(joining("\n"));
        }

        public static HashSlotsStats fromKeys(List<RedisMarketKey> keys) {
            Map<Integer, Long> grouppedKeys = keys
                    .stream()
                    .collect(groupingBy(
                            RedisMarketKey::getHashSlot,
                            Collectors.counting()
                    ));

            return new HashSlotsStats(grouppedKeys);
        }
    }

    public static PartitionsStats fromKeys(
            Set<RedisMarketKey> keys,
            Function<Object, Partition> toPartition
    ) {
        Map<Partition, List<RedisMarketKey>> grouppedKeys = keys
                .stream()
                .collect(groupingBy(toPartition));

        Map<Partition, HashSlotsStats> hashSlotKeyCount = grouppedKeys
                .entrySet()
                .stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> HashSlotsStats.fromKeys(entry.getValue())
                ));

        return new PartitionsStats(hashSlotKeyCount);
    }
}
