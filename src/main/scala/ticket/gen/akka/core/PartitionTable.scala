package ticket.gen.akka.core

import akka.cluster.Member
import ticket.gen.akka.core.PartitionTable.{Change, Add, Del}

import scala.collection.immutable.{Map, Set}

object PartitionTable {
  sealed trait Change
  case class Add(m: Member, pId: Int) extends Change
  case class Del(m: Member, pId: Int) extends Change
}

class PartitionTable(numOfPartitions: Int, members: Set[Member], partitionIdMember: Map[Int, Member]) {
  def apply(pId: Int) = partitionIdMember(pId)

  def addMemberAndRebalance(newMember: Member): PartitionTable = {
    newPartitionTable(rebalance(members + newMember))
  }

  def removeMemberAndRebalance(oldMember: Member): PartitionTable = {
    newPartitionTable(rebalance(members - oldMember))
  }

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
  def diff(newPartitionTable: PartitionTable): List[Change] = partitionIdMember
    .flatMap({case (pId, oldM) => {
      val newM = newPartitionTable(pId)
      if (oldM == newM) List() else List(Del(oldM, pId), Add(newM, pId))
    }})
    .toList

  //Helpers:

  def newPartitionTable(x: (Set[Member], Map[Int, Member])): PartitionTable = {
    PartitionTable(numOfPartitions, x._1, x._2)
  }

  def rebalance(newMembers: Set[Member]): (Set[Member], Map[Int, Member]) = {
    //members ordered in-line starting with 0
    val idxMember = Seq.range(0, newMembers.size).zip(newMembers).toMap
    val partitionIdToMember: Int => Member = pId => idxMember(pId % members.size)

    val newPartitionIdMember = Seq
      .range(0, numOfPartitions)
      .map(pId => (pId, partitionIdToMember(pId)))
      .toMap

    (newMembers, newPartitionIdMember)
  }
}
