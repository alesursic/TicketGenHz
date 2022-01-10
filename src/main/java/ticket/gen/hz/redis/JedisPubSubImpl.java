package ticket.gen.hz.redis;

import com.hazelcast.collection.ISet;
import redis.clients.jedis.JedisPubSub;
import ticket.gen.hz.state.RedisMarketKey;

public class JedisPubSubImpl extends JedisPubSub {
    private static final String KEYSPACE_PREFIX = "__keyspace@0__:";

    private final ISet<RedisMarketKey> distributedKeyspace;

    public JedisPubSubImpl(ISet<RedisMarketKey> distributedKeyspace) {
        this.distributedKeyspace = distributedKeyspace;
    }

    /**
     * On psubscribe for keyspace notifications
     *
     * @param pattern "__keyspace@0__:{develop:bk:0}*"
     * @param channel "__keyspace@0__:{develop:bk:0}:uof:1/sr:match:21796437/16/hcp=-2.25"
     * @param message "hset"
     */
    @Override
    public void onPMessage(String pattern, String channel, String message) {
        if (message.equals("hset")) {
            String changedKey = channel.split(KEYSPACE_PREFIX)[1];
            distributedKeyspace.add(RedisMarketKey.parse(changedKey));
        }
        //todo: implement deletion
    }
}