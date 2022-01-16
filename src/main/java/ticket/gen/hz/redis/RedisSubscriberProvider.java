package ticket.gen.hz.redis;

import com.hazelcast.collection.ISet;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import ticket.gen.hz.state.RedisMarketKey;

import java.util.function.Function;
import java.util.function.Supplier;

public class RedisSubscriberProvider implements Function<HashTag, LazyRedisSubscriber> {
    private final ISet<RedisMarketKey> distributedKeyspace;
    private final JedisCluster jedisCluster;

    public RedisSubscriberProvider(ISet<RedisMarketKey> distributedKeyspace, JedisCluster jedisCluster) {
        this.distributedKeyspace = distributedKeyspace;
        this.jedisCluster = jedisCluster;
    }

    private Jedis getConnectionFromSlot(int hashSlot) {
        return jedisCluster.getConnectionFromSlot(hashSlot);
    }

    @Override
    public LazyRedisSubscriber apply(HashTag hashTag) {
        final Supplier<Jedis> jedisSupplier = () -> getConnectionFromSlot(hashTag.hashSlot());

        return new LazyRedisSubscriber(
                hashTag,
                jedis -> {
                    try {
                        jedis.psubscribe(new JedisPubSubImpl(distributedKeyspace), hashTag.keyspaceTopic());
                    } finally {
                        jedis.close(); //idempotent method
                    }
                },
                jedisSupplier
        );
    }
}
