package ticket.gen.hz.state;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.PartitionAware;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

//Example: "{staging:bk:22017}:uof:1/sr:match:21796437/16/hcp=-2.25"
public class RedisMarketKey implements DataSerializable, PartitionAware<Integer> {
    //Serializable
    private String rawKey;
    private String env;
    private int bookmakerId;
    private int producerId;
    private String eventId;
    private int marketId;
    private String[] specifiers;
    private boolean isMetaData;
    //Additional (non-serializable)
    private int partitionKey;
    private String hashTag;

    public RedisMarketKey() {
    }

    public RedisMarketKey(
            String rawKey,
            String env,
            int bookmakerId,
            int producerId,
            String eventId,
            int marketId,
            String[] specifiers,
            boolean isMetaData
    ) {
        this.rawKey = rawKey;
        this.env = env;
        this.bookmakerId = bookmakerId;
        this.producerId = producerId;
        this.eventId = eventId;
        this.marketId = marketId;
        this.specifiers = specifiers;
        this.isMetaData = isMetaData;

        this.hashTag = toHashTag(env, bookmakerId, isMetaData);
        this.partitionKey = JedisClusterCRC16.getSlot(hashTag);
    }

    @Override
    public Integer getPartitionKey() {
        return partitionKey;
    }

    public int getHashSlot() {
        return partitionKey;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(rawKey);
        out.writeString(env);
        out.writeInt(bookmakerId);
        out.writeInt(producerId);
        out.writeString(eventId);
        out.writeInt(marketId);
        out.writeStringArray(specifiers);
        out.writeBoolean(isMetaData);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        rawKey = in.readString();
        env = in.readString();
        bookmakerId = in.readInt();
        producerId = in.readInt();
        eventId = in.readString();
        marketId = in.readInt();
        specifiers = in.readStringArray();
        isMetaData = in.readBoolean();

        this.hashTag = toHashTag(env, bookmakerId, isMetaData);
        partitionKey = JedisClusterCRC16.getSlot(hashTag);
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public int getBookmakerId() {
        return bookmakerId;
    }

    public void setBookmakerId(int bookmakerId) {
        this.bookmakerId = bookmakerId;
    }

    public int getProducerId() {
        return producerId;
    }

    public void setProducerId(int producerId) {
        this.producerId = producerId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public int getMarketId() {
        return marketId;
    }

    public void setMarketId(int marketId) {
        this.marketId = marketId;
    }

    public String[] getSpecifiers() {
        return specifiers;
    }

    public void setSpecifiers(String[] specifiers) {
        this.specifiers = specifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisMarketKey that = (RedisMarketKey) o;
        return bookmakerId == that.bookmakerId &&
                isMetaData == that.isMetaData &&
                producerId == that.producerId &&
                marketId == that.marketId &&
                Objects.equals(env, that.env) &&
                Objects.equals(eventId, that.eventId) &&
                Arrays.equals(specifiers, that.specifiers);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(env, bookmakerId, isMetaData, producerId, eventId, marketId);
        result = 31 * result + Arrays.hashCode(specifiers);
        return result;
    }

    @Override
    public String toString() {
        return rawKey;
    }

    //Helpers:

    //todo: unit test
    public static RedisMarketKey parse(String input) {
        //In the hash tag:
        String[] initParts = input.split("}:");
        String hashTag = initParts[0].split("\\{")[1];
        String[] hashTagParts = hashTag.split(":");
        String env = hashTagParts[0];
        int bookmakerId = Integer.parseInt(hashTagParts[2]);
        boolean isMetaData = hashTagParts[1].equals("m");

        //After hash tag:
        String[] parts = initParts[1].split("/");
        int producerId = Integer.parseInt(parts[0].replaceAll("uof:", ""));
        String eventId = parts[1];
        int marketId = Integer.parseInt(parts[2]);
        String[] specifiers = Arrays
                .stream(parts)
                .skip(3)
                .toArray(String[]::new);

        return new RedisMarketKey(input, env, bookmakerId, producerId, eventId, marketId, specifiers, isMetaData);
    }

    //todo: refactor
    private static String toHashTag(String env, int bookmakerId, boolean isMetaData) {
        return String.format(
                "%s:%s:%d",
                env,
                isMetaData ? "m" : "bk",
                bookmakerId
        );
    }
}