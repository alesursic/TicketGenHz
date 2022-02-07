package ticket.gen.akka.typed.core

import akka.actor.typed.ActorRef
import akka.cluster.Member
import ticket.gen.akka.typed.core.PartitionTable.{Add, Change, Del, SetDispatcherRef}
import ticket.gen.akka.typed.setactors.SetDispatcher.Command

import scala.collection.immutable.{Map, Set}

object PartitionTable {
  type SetDispatcherRef = ActorRef[Command]
  
  sealed trait Change
  case class Add(m: SetDispatcherRef, pId: Int) extends Change
  case class Del(m: SetDispatcherRef, pId: Int) extends Change

  val EMPTY = PartitionTable(8, Set(), Map())
}

class PartitionTable(
  numOfPartitions: Int, 
  members: Set[SetDispatcherRef], 
  partitionIdMember: Map[Int, SetDispatcherRef]
) {
  def apply(pId: Int) = partitionIdMember(pId)

  def addMemberAndRebalance(newMember: SetDispatcherRef): PartitionTable = {
    val newMembers = members + newMember
    newPartitionTable(newMembers, rebalance(newMembers))
  }

  def removeMemberAndRebalance(oldMember: SetDispatcherRef): PartitionTable = {
    val newMembers = members - oldMember
    newPartitionTable(newMembers, rebalance(newMembers))
  }

  def containsMember(m: SetDispatcherRef): Boolean = {
    members.contains(m)
  }

  def numOfMembers(): Int = members.size

  def getPartitionIdMember(): Map[Int, SetDispatcherRef] = partitionIdMember

  /**
   * Calcluates difference between old and new partition tables
   *
   * NOTE: Partition may either stay in the same member or be removed from one member and be added to another member
   *
   * Example:
   *  new: {m0: [p0], m1: [p1]} == {p0: m0, p1: m1}
   *  old: {m0: [p0, p1]} == {p0: m0, p1: m0}
   *  diff: [Del(m0, p1), Add(m1, p1)]
   *
   * @param newPartitionTable
   * @return list of added/removed partitions to/from members
   */
  def diff(newPartitionTable: PartitionTable): List[Change] = {
    if (partitionIdMember.isEmpty) {
      //virgin case (first member of the cluster has joined)
      newPartitionTable
        .getPartitionIdMember()
        .map({case (pId, m) => Add(m, pId)})
        .toList
    } else {
      partitionIdMember
        .flatMap({case (pId, oldM) => {
          val newM = newPartitionTable(pId)
          if (oldM == newM) List() else List(Del(oldM, pId), Add(newM, pId))
        }})
        .toList
    }
  }

  //Helpers:

  def newPartitionTable(x: (Set[SetDispatcherRef], Map[Int, SetDispatcherRef])): PartitionTable = {
    PartitionTable(numOfPartitions, x._1, x._2)
  }

  def rebalance(newMembers: Set[SetDispatcherRef]): Map[Int, SetDispatcherRef] = {
    //members ordered in-line starting with 0
    val idxMember = Seq.range(0, newMembers.size).zip(newMembers).toMap
    val partitionIdToMember: Int => SetDispatcherRef = pId => idxMember(pId % newMembers.size)

    Seq
      .range(0, numOfPartitions)
      .map(pId => (pId, partitionIdToMember(pId)))
      .toMap
  }
}
