package ticket.gen.akka.typed.myredis

import akka.actor.typed.ActorRef
import ticket.gen.hz.helpers.ThreadUtil
import redis.clients.jedis.{HostAndPort, JedisCluster}
import ticket.gen.hz.redis.{HashTag, LazyRedisSubscriber}
import org.slf4j.Logger
import ticket.gen.akka.typed.setactors.SetDispatcher

object SubscribeToRedis {
  def apply(hashTag: HashTag, logger: Logger, setActor: ActorRef[SetDispatcher.Command]): Unit = {
      val jedisCluster = new JedisCluster(new HostAndPort("localhost", 7000)) //move to application.conf
      val lazyRedisSubscriber = new LazyRedisSubscriber(
        hashTag,
        jedis => {
          try {
            jedis.psubscribe(
              new JedisPubSubImpl(setActor),
              hashTag.keyspaceTopic()
            )
          } finally {
            jedis.close(); //idempotent method
          }
        },
        () => jedisCluster.getConnectionFromSlot(hashTag.hashSlot())
      );

      ThreadUtil.subscribeToRedis(
        logger,
        lazyRedisSubscriber,
        "on-bootstrap"
      )
  }
}
