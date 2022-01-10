package ticket.gen.hz.helpers;

import com.hazelcast.partition.Partition;

import java.util.function.Function;

public class HashTagToPartition implements Function<HashTag, Partition> {
    private final Function<Object, Partition> toPartition;

    public HashTagToPartition(Function<Object, Partition> toPartition) {
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
