package ticket.gen.hz.helpers;

import com.hazelcast.partition.Partition;
import ticket.gen.hz.redis.HashTag;

import java.util.function.Function;

public class HashTagToPartitionF implements Function<HashTag, Partition> {
    private final Function<Object, Partition> toPartition;

    public HashTagToPartitionF(Function<Object, Partition> toPartition) {
        this.toPartition = toPartition;
    }

    public Partition getPartition(HashTag hashTag) {
        return toPartition.apply(hashTag.hashSlot());
    }

    @Override
    public Partition apply(HashTag o) {
        return getPartition(o);
    }
}
