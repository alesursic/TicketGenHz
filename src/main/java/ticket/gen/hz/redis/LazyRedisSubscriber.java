package ticket.gen.hz.redis;

import redis.clients.jedis.Jedis;
import ticket.gen.hz.helpers.HashTag;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class LazyRedisSubscriber implements Runnable {
    private final HashTag hashTag;
    private final Consumer<Jedis> pubsubTask;
    private final Supplier<Jedis> jedisProvider;

    private volatile Jedis jedis;

    public LazyRedisSubscriber(
            HashTag hashTag,
            Consumer<Jedis> pubsubTask,
            Supplier<Jedis> jedisProvider
    ) {
        this.hashTag = hashTag;
        this.pubsubTask = pubsubTask;
        this.jedisProvider = jedisProvider;
    }

    @Override
    public void run() {
        jedis = jedisProvider.get();
        pubsubTask.accept(jedis);
    }

    public HashTag getHashTag() {
        return hashTag;
    }

    public void interrupt() {
        //disconnect only works if connection has been fully initialized
        if (!jedis.isConnected()) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); //reset the interrupt flag
            }
        }
        jedis.disconnect(); //not jedis.close()! (because this just returns connection to the pool)
    }
}
