package ticket.gen.akka.untyped.core

import akka.actor.*
import akka.cluster.ClusterEvent.*
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.remote.RemoteScope
import org.checkerframework.checker.units.qual.m
import ticket.gen.akka.untyped.core.PartitionTable.{Add, Change, Del}
import ticket.gen.akka.untyped.set.SetDispatcher
import ticket.gen.akka.untyped.set.SetDispatcher.{AddSetActor, RemoveSetActor}
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck, Publish, Unsubscribe}
import ticket.gen.akka.untyped.core.ClusterEventActor.{TOPIC, PartitionTableBroadcast}

import scala.collection.mutable.{Map, Set}

object ClusterEventActor {
  val TOPIC = "registration"
  case class PartitionTableBroadcast(in: PartitionTable)
}

class ClusterEventActor(var partitionTable: PartitionTable) extends Actor with ActorLogging {
  var isLeader = false
  val members: Set[Address] = Set()

  context.actorOf(Props(classOf[SetDispatcher])) //local "deployment" and rebalance
  cluster().subscribe(self, classOf[ClusterDomainEvent])
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe(TOPIC, self)

  def receive = {
    case MemberUp(member) if isLeader =>
      log.info(s"$member UP.")
      members.add(member.address)
      val newPartitionTable = PartitionTable(members.toSet)
      mediator ! Publish(TOPIC, PartitionTableBroadcast(newPartitionTable))

    case MemberExited(member) if isLeader =>
      log.info(s"$member EXITED.")
      members.remove(member.address)
      val newPartitionTable = PartitionTable(members.toSet)
      mediator ! Publish(TOPIC, PartitionTableBroadcast(newPartitionTable))

    case LeaderChanged(leader) =>
      leader.foreach(member => log.info(s"$member NEW_LEADER."))
      isLeader = leader.map(_ == cluster().selfAddress).getOrElse(false)
      if (isLeader) {
        members.add(leader.get)
        val newPartitionTable = PartitionTable(members.toSet)
        mediator ! Publish(TOPIC, PartitionTableBroadcast(newPartitionTable))
      }

    case PartitionTableBroadcast(newPartitionTable) =>
      log.info("Received partition table broadcast, will start actual rebalance")
      val changes = partitionTable.diff(newPartitionTable)
      //todo: apply changes for the owned partitions

    case other =>
      log.info("Cluster event {} not in my interest..", other)
  }

  override def postStop(): Unit = {
    cluster().unsubscribe(self)
    super.postStop()
  }

  //Helpers:

  def cluster() = Cluster(context.system)
}
