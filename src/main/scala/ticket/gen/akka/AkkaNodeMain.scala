package ticket.gen.akka

import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.Cluster
import com.example.iot.IotSupervisor
import ticket.gen.akka.core.RedisKeysActor
import ticket.gen.akka.redis1.JedisPubSubImpl
import ticket.gen.hz.helpers.ThreadUtil
import ticket.gen.hz.redis.{HashTag, LazyRedisSubscriber}
import ticket.gen.hz.state.RedisMarketKey

object AkkaNodeMain {
  def main(args: Array[String]): Unit = {
    val hashTag = HashTag.fromString(System.getenv("HASH_TAG"))

    //################################ Akka ################################
    val sys = ActorSystem[RedisMarketKey](RedisKeysActor(), "RedisKeysSys")
    val cluster = Cluster(sys)
    //################################ Akka ################################

    //################################ Redis ################################
    val jedisCluster = new JedisCluster(new HostAndPort("localhost", 7000))
    val lazyRedisSubscriber = new LazyRedisSubscriber(
      hashTag,
      jedis => {
        try {
          jedis.psubscribe(
            new JedisPubSubImpl(sys.ref),
            hashTag.keyspaceTopic()
          )
        } finally {
          jedis.close(); //idempotent method
        }
      },
      () => jedisCluster.getConnectionFromSlot(hashTag.hashSlot())
    );

    ThreadUtil.subscribeToRedis(
      sys.log,
      lazyRedisSubscriber,
      "on-bootstrap"
    )
    //################################ Redis ################################
  }
}
