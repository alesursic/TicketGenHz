package ticket.gen.akka.untyped.core

import aia.cluster.words.JobWorker
import akka.actor.{Actor, ActorLogging, Deploy, Props, ActorRef}
import akka.cluster.ClusterEvent.*
import akka.cluster.{Cluster, MemberStatus}
import akka.remote.RemoteScope
import ticket.gen.akka.untyped.setactors.SetDispatcher.{AddSetActor, RemoveSetActor}
import ticket.gen.akka.untyped.core.PartitionTable.*
import ticket.gen.akka.untyped.setactors.SetDispatcher

class ClusterEventActor(var partitionTable: PartitionTable) extends Actor with ActorLogging {
  Cluster(context.system).subscribe(self, classOf[ClusterDomainEvent])
  //local deployment
  rebalance(context.actorOf(Props(classOf[SetDispatcher])))

  def receive = {
    case MemberUp(member) =>
      log.info(s"$member UP.")
      rebalance(
        //remote deployment of actor
        context.actorOf(
          Props[SetDispatcher]().withDeploy(
            Deploy(scope = RemoteScope(member.address))
          )
        )
      )
    case _ => log.info("Cluster event not in my interest..")
  }

  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
    super.postStop()
  }

  //Helpers:

  def rebalance(newActorRef: ActorRef): Unit = {
    //calculate how to rebalance partitions (new partitions state)
    val newPartitionTable = partitionTable.addMemberAndRebalance(newActorRef)
    val changes: List[Change] = partitionTable.diff(newPartitionTable)
    partitionTable = newPartitionTable //actor's state mutation

    //instruct other nodes to move partitions (perform rebalance)
    changes foreach {
      case Add(m, pId) => m ! AddSetActor(pId)
      case Del(m, pId) => m ! RemoveSetActor(pId)
    }
  }
}
