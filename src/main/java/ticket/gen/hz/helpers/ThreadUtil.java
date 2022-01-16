package ticket.gen.hz.helpers;

import org.slf4j.Logger;
import ticket.gen.hz.redis.LazyRedisSubscriber;

public class ThreadUtil {
    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void subscribeToRedis(
            Logger log,
            LazyRedisSubscriber redisSubscriber,
            String prefix
    ) {
        log.info(
                "{} connecting to {}",
                prefix,
                redisSubscriber.getHashTag()
        );

        Thread t = new Thread(redisSubscriber);
        t.start();
    }
}
