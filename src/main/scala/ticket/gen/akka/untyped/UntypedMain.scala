package ticket.gen.akka.untyped

import akka.actor.*
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import ticket.gen.akka.untyped.core.{ClusterEventActor, PartitionTable}

object UntypedMain {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val system = ActorSystem("RedisKeysSys", config)

    system.actorOf(
      ClusterSingletonManager.props(
        Props(classOf[ClusterEventActor], PartitionTable.EMPTY),
        terminationMessage = "end",
        settings = ClusterSingletonManagerSettings(system),
      ),
      "cluster-event-actor"
    )
  }
}
