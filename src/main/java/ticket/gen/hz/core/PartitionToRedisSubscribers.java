package ticket.gen.hz.core;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.partition.Partition;
import ticket.gen.hz.redis.LazyRedisSubscriber;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class PartitionToRedisSubscribers {
    private final Map<Partition, Set<LazyRedisSubscriber>> partitionToRedisSubscribers;

    public PartitionToRedisSubscribers(Map<Partition, Set<LazyRedisSubscriber>> partitionToRedisSubscribers) {
        this.partitionToRedisSubscribers = partitionToRedisSubscribers;
    }

    public Set<LazyRedisSubscriber> getRedisSubscribers(int partitionId) {
        return getEntry(partitionId)
                .map(Map.Entry::getValue)
                .orElse(ImmutableSet.of());
    }

    public boolean containsPartition(int partitionId) {
        return getEntry(partitionId).isPresent();
    }

    private Optional<Map.Entry<Partition, Set<LazyRedisSubscriber>>> getEntry(int partitionId) {
        return stream(partition -> partition.getPartitionId() == partitionId)
                .findFirst();
    }

    public Stream<Map.Entry<Partition, Set<LazyRedisSubscriber>>> streamAll() {
        return stream(ignore -> true);
    }

    public Stream<Map.Entry<Partition, Set<LazyRedisSubscriber>>> streamLocal() {
        return stream(partition -> partition.getOwner().localMember());
    }

    public Stream<Map.Entry<Partition, Set<LazyRedisSubscriber>>> streamForeign() {
        return stream(partition -> !partition.getOwner().localMember());
    }

    public Stream<Map.Entry<Partition, Set<LazyRedisSubscriber>>> stream(Predicate<Partition> p) {
        return partitionToRedisSubscribers
                .entrySet()
                .stream()
                .filter(entry -> p.test(entry.getKey()));
    }

    //Additional:

    //    public boolean isPartitionOwned(int partitionId) {
//        return getEntry(partitionId)
//                .map(Map.Entry::getKey)
//                .map(Partition::getOwner)
//                .map(Member::localMember)
//                .orElse(false);
//    }

}
