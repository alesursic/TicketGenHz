package ticket.gen.hz.core;

import com.hazelcast.cluster.Member;
import com.hazelcast.partition.Partition;
import ticket.gen.hz.helpers.HashTag;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class PartitionToHashTags {
    private final Map<Partition, Set<HashTag>> partitionToHashTags;

    public PartitionToHashTags(Map<Partition, Set<HashTag>> partitionToHashTags) {
        this.partitionToHashTags = partitionToHashTags;
    }

    public boolean isPartitionOwned(int partitionId) {
        return partitionToHashTags
                .keySet()
                .stream()
                .filter(partition -> partition.getPartitionId() == partitionId)
                .map(Partition::getOwner)
                .map(Member::localMember)
                .findFirst()
                .orElse(false);
    }

    public Stream<Map.Entry<Partition, Set<HashTag>>> stream() {
        return partitionToHashTags.entrySet().stream();
    }

    public Set<Integer> ownedPartitionIds() {
        return partitionToHashTags
                .keySet()
                .stream()
                .filter(partition -> partition.getOwner().localMember())
                .map(Partition::getPartitionId)
                .collect(toSet());
    }
}
