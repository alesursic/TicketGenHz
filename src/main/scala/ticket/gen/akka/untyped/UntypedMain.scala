package ticket.gen.akka.untyped

import akka.actor.*
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import ticket.gen.akka.untyped.core.{ClusterEventActor, PartitionTable, PubSubActor}

object UntypedMain {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val system = ActorSystem("RedisKeysSys", config)

    system.actorOf(
        Props(classOf[ClusterEventActor], PartitionTable.EMPTY),
        "cluster-event-actor"
    )
  }
}
