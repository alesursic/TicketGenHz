package ticket.gen.akka.typed

import akka.actor.typed.ActorSystem
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.typed.{Cluster, Subscribe}
import ticket.gen.akka.typed.core.MemberEventActor

object AkkaNodeMain {
  def main(args: Array[String]): Unit = {
    val sys = ActorSystem(MemberEventActor(), "RedisKeysSys")
    val cluster = Cluster(sys)
    cluster.subscriptions ! Subscribe(sys.ref, classOf[MemberEvent])
    //    val cluster = ClusterSingleton(sys)
    //    cluster.init(SingletonActor(
    //      Behaviors.supervise(SetDispatcherGuardian()).onFailure[Exception](SupervisorStrategy.restart),
    //      "SetDispatcherGuardian"
    //    ))
  }
}
