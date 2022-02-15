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
import fj.data.Set.member
import ticket.gen.akka.untyped.core.ClusterEventActor.{IDENTIFY_ID, PartitionTableBroadcast, TOPIC}

import scala.collection.mutable.{Map, Set}

object ClusterEventActor {
  val IDENTIFY_ID = 1
  val TOPIC = "registration"
  case class PartitionTableBroadcast(in: PartitionTable)
}

class ClusterEventActor(var partitionTable: PartitionTable) extends Actor with ActorLogging {
  var isLeader = false
  val members: Map[Address, ActorRef] = Map()

  val setDispatcher = context.actorOf(Props(classOf[SetDispatcher])) //local "deployment" and rebalance
  cluster().subscribe(self, classOf[ClusterDomainEvent])

  def receive = {
    case ActorIdentity(`IDENTIFY_ID`, Some(remoteClusterEventActor)) if isLeader =>
      log.info("Actor {} identified", remoteClusterEventActor)
      members.put(remoteClusterEventActor.path.address, remoteClusterEventActor)
      val newPartitionTable = PartitionTable(members.keys.toList.sorted)
      broadcast(newPartitionTable)

    case MemberUp(member) if isLeader =>
      log.info(s"$member UP.")
      remoteClusterEventActor(member.address) ! Identify(IDENTIFY_ID)

    case MemberExited(member) if isLeader =>
      log.info(s"$member EXITED.")
      members.remove(member.address)
      val newPartitionTable = PartitionTable(members.keys.toList.sorted)
      //When leader is exiting, it publishes the partition table to the new leader (very important)
      broadcast(newPartitionTable) //including itself!

    case LeaderChanged(leader) =>
      leader.foreach(member => log.info(s"$member LEADER_CHANGED."))
      isLeader = leader.contains(cluster().selfAddress)
      if (isLeader) {
        /*
         * Node can become a leader in these cases:
         *  1. Bootstrap of the application (only itself is a cluster member)
         *  2. Previous leader exited the cluster (membership list is empty & partition table was transferred on exit)
         *  3. Leadership transfer (split-brain) (membership list is empty & partition table may not have been transferred (yet))
         */
        val membersTemp = cluster().state.members

        if (membersTemp.size == 1) {
          members.put(cluster().selfAddress, context.self)
        } else if (partitionTable.isEmpty()) {
          //members discovery

        } else {

        }

        val newPartitionTable = PartitionTable(members.keys.toList.sorted)
        broadcast(newPartitionTable)


        //NOTE: Leadership may be stolen from a different node. In this case NOOP because ex-leader will
        //send us the new partition table (my partition table doesn't contain all members).
        //todo: detect stealing leadership and skip broadcast
      }

    case PartitionTableBroadcast(newPartitionTable) =>
      log.info("Received partition table broadcast, will start actual rebalance")
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

  def remoteClusterEventActor(address: Address): ActorSelection =
    context.actorSelection(
      String.format(
        "%s/user/cluster-event-actor",
        address
      )
    )

  def broadcast(newPartitionTable: PartitionTable): Unit =
    members.values.foreach(_ ! PartitionTableBroadcast(newPartitionTable))
}
