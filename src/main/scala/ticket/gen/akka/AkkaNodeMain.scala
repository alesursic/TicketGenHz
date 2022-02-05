package ticket.gen.akka

import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import akka.actor.typed.ActorSystem
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.typed.{Cluster, Subscribe}
import com.example.iot.IotSupervisor
import ticket.gen.akka.core.SetDispatcherGuardian
import ticket.gen.akka.redis1.JedisPubSubImpl
import ticket.gen.akka.setactors.SetDispatcher
import ticket.gen.akka.setactors.SetDispatcher.Command
import ticket.gen.hz.helpers.ThreadUtil
import ticket.gen.hz.redis.{HashTag, LazyRedisSubscriber}
import ticket.gen.hz.state.RedisMarketKey

object AkkaNodeMain {
  def main(args: Array[String]): Unit = {
//    val hashTag = HashTag.fromString(System.getenv("HASH_TAG"))

    //################################ Akka ################################
    val sys = ActorSystem[Receptionist.Listing](SetDispatcherGuardian(), "RedisKeysSys")
    val cluster = Cluster(sys)

    //cluster.subscriptions ! Subscribe(null, classOf[MemberEvent])

//    cluster.registerOnMemberUp(() => {
      //create redis connections
      //create set dispatcher
      //create set actors (if this is second or later member then some sub-set actors may be duplicates)
      //num(redis-connections) == num(set actors)

      //send messages to other set dispatchers to kill some of their set actors (duplicates)
//    })
    //################################ Akka ################################

    //################################ Redis ################################
//    val jedisCluster = new JedisCluster(new HostAndPort("localhost", 7000))
//    val lazyRedisSubscriber = new LazyRedisSubscriber(
//      hashTag,
//      jedis => {
//        try {
//          jedis.psubscribe(
//            new JedisPubSubImpl(null), //sys.ref),
//            hashTag.keyspaceTopic()
//          )
//        } finally {
//          jedis.close(); //idempotent method
//        }
//      },
//      () => jedisCluster.getConnectionFromSlot(hashTag.hashSlot())
//    );
//
//    ThreadUtil.subscribeToRedis(
//      sys.log,
//      lazyRedisSubscriber,
//      "on-bootstrap"
//    )
    //################################ Redis ################################
  }
}
