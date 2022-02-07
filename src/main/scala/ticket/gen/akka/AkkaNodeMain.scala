package ticket.gen.akka

import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import akka.actor.typed.{ActorSystem, SupervisorStrategy, ActorRef}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.typed.{Cluster, ClusterSingleton, SingletonActor, Subscribe}
import com.example.iot.IotSupervisor
import com.example.singleton.Counter
import ticket.gen.akka.core.SetDispatcherGuardian
import ticket.gen.akka.myredis.JedisPubSubImpl
import ticket.gen.akka.setactors.SetDispatcher
import ticket.gen.akka.setactors.SetDispatcher.Command
import ticket.gen.hz.helpers.ThreadUtil
import ticket.gen.hz.redis.{HashTag, LazyRedisSubscriber}
import ticket.gen.hz.state.RedisMarketKey

object AkkaNodeMain {
  def main(args: Array[String]): Unit = {
    val sys = ActorSystem(Behaviors.empty, "RedisKeysSys")
    val cluster = ClusterSingleton(sys)
    cluster.init(SingletonActor(
      Behaviors.supervise(SetDispatcherGuardian()).onFailure[Exception](SupervisorStrategy.restart),
      "SetDispatcherGuardian"
    ))
  }
}
