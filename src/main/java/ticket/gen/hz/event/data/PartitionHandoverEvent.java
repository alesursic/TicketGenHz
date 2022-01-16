package ticket.gen.hz.event.data;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class PartitionHandoverEvent implements DataSerializable {
    private PartitionReplica dest;
    private int partitionId;

    public PartitionHandoverEvent() {
    }

    public PartitionHandoverEvent(PartitionReplica dest, int partitionId) {
        this.dest = dest;
        this.partitionId = partitionId;
    }

    public PartitionReplica getDest() {
        return dest;
    }

    public void setDest(PartitionReplica dest) {
        this.dest = dest;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public String toString() {
        return "PartitionHandoverEvent{" +
                "dest=" + dest +
                ", partitionId=" + partitionId +
                '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(dest);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dest = in.readObject();
        partitionId = in.readInt();
    }

    //Helpers:

    public static PartitionHandoverEvent create(Member member, int partitionId) {
        return new PartitionHandoverEvent(
                new PartitionReplica(member.getAddress(), member.getUuid()),
                partitionId
        );
    }
}
