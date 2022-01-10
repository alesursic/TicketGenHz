package ticket.gen.hz.helpers;

import redis.clients.jedis.util.JedisClusterCRC16;

public class HashTag {
    private final String env;
    private final boolean isMetaData;
    private final int bookmakerId;

    public HashTag(String env, boolean isMetaData, int bookmakerId) {
        this.env = env;
        this.isMetaData = isMetaData;
        this.bookmakerId = bookmakerId;
    }

    public int hashSlot() {
        return JedisClusterCRC16.getSlot(toString());
    }

    @Override
    public String toString() {
        return String.format(
                "{%s:%s:%d}",
                env,
                isMetaData ? "m" : "bk",
                bookmakerId
        );
    }

    public String keyspaceTopic() {
        return String.format("__keyspace@0__:%s*", toString());
    }

    @Override
    public int hashCode() {
        return hashSlot();
    }

    public static HashTag fromString(String input) {
        String[] initParts = input.split("}");
        String hashTag = initParts[0].split("\\{")[1];
        String[] hashTagParts = hashTag.split(":");
        String env = hashTagParts[0];
        boolean isMetaData = hashTagParts[1].equals("m");
        int bookmakerId = Integer.parseInt(hashTagParts[2]);

        return new HashTag(env, isMetaData, bookmakerId);
    }
}
