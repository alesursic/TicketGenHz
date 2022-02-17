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
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck, Unsubscribe}
import ticket.gen.akka.untyped.core.ActorRefDiscoveryJob.DiscoveryResult
import ticket.gen.akka.untyped.core.ClusterEventActor.{Changes, IDENTIFY_ID, PartitionTableBroadcast}

import scala.collection.immutable
import scala.collection.mutable.{Map, Set}

object ClusterEventActor {
  val IDENTIFY_ID = 1
  case class PartitionTableBroadcast(in: PartitionTable)
  case class Changes(in: List[Change])
}

class ClusterEventActor(var partitionTable: PartitionTable) extends Actor with ActorLogging {
  var isLeader = false
  var members: Map[Address, ActorRef] = Map()

  val setDispatcher = context.actorOf(Props(classOf[SetDispatcher])) //local "deployment" and rebalance
  cluster().subscribe(self, classOf[ClusterDomainEvent])

  def receive = {
    case MemberUp(member) if isLeader =>
      log.info(s"$member UP.")
      context.actorOf(Props(classOf[ActorRefDiscoveryJob], immutable.Set(member), self))

    case MemberExited(member) if isLeader =>
      log.info(s"$member EXITED.")
      if (member.address != self.path.address) {
        members.remove(member.address)
        broadcast(PartitionTable(members.keys.toList.sorted))
      }
    //else: no need to publish, will be auto-detected by new leader

    case LeaderChanged(leader) =>
      leader.foreach(member => log.info(s"$member LEADER_CHANGED."))
      isLeader = leader.contains(cluster().selfAddress)
      if (isLeader) {
        log.info("NEW_LEADER in the cluster {}", leader.get)
        /*
         * Node can become a leader in these cases:
         *  1. Bootstrap of the application (only itself is a cluster member)
         *  2. Previous leader exited the cluster
         *  3. Leadership transfer to the first node in seeds list (split-brain)
         */
        val currMembers = cluster().state.members.filter(_.status == MemberStatus.Up)
        context.actorOf(Props(classOf[ActorRefDiscoveryJob], currMembers, self))
      }

    case DiscoveryResult(newMembers: Map[Address, ActorRef]) if isLeader =>
      log.info("DISCOVERY.")
      members.addAll(newMembers)
      broadcast(PartitionTable(members.keys.toList.sorted))

    case PartitionTableBroadcast(newPartitionTable) =>
      log.info("PARTITION_TABLE_BROADCAST.")
      val changes = partitionTable.diff(newPartitionTable)
      partitionTable = newPartitionTable
      changes
          .filter(_.getAddress() == cluster().selfAddress) //apply only for owned partitions
          .foreach({
            case Add(addr, pId) =>
              setDispatcher ! AddSetActor(pId)
            case Del(addr, pId) =>
              setDispatcher ! RemoveSetActor(pId)
          })

    case other =>
      log.info("Cluster event {} not in my interest..", other)
  }

  override def postStop(): Unit = {
    cluster().unsubscribe(self)
    super.postStop()
  }

  //Helpers:

  def cluster() = Cluster(context.system)

  def broadcast(newPartitionTable: PartitionTable): Unit =
    members.values.foreach(_ ! PartitionTableBroadcast(newPartitionTable))

  def broadcast(changes: List[Change]): Unit =
    members.values.foreach(_ ! Changes(changes))
}
