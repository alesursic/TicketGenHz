package ticket.gen.akka.untyped.core

import akka.actor.*
import ticket.gen.akka.untyped.core.PartitionTable.{NUM_OF_PARTITIONS}
import ticket.gen.akka.untyped.core.PartitionChange.{Add, Change, Del}

import scala.collection.immutable.{Map, Set}

object PartitionTable {
  val NUM_OF_PARTITIONS = 8 //todo: extract from config
  val EMPTY = new PartitionTable(Map())

  def apply(newMembers: List[Address]): PartitionTable = {
    val idxMember = Seq.range(0, newMembers.size).zip(newMembers).toMap
    val partitionIdToMember: Int => Address = pId => idxMember(pId % newMembers.size)

    val newPartitionIdToMember = Seq
      .range(0, NUM_OF_PARTITIONS)
      .map(pId => (pId, partitionIdToMember(pId)))
      .toMap

    new PartitionTable(newPartitionIdToMember)
  }
}

class PartitionTable(partitionIdToMember: Map[Int, Address]) extends Serializable {
  def apply(pId: Int) = partitionIdToMember(pId)

  def getPartitionIdToMember() = partitionIdToMember
  
  def isEmpty() = partitionIdToMember.isEmpty

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
    if (partitionIdToMember.isEmpty) {
      //virgin case (first member of the cluster has joined)
      newPartitionTable
        .getPartitionIdToMember()
        .map({case (pId, m) => Add(m, pId)})
        .toList
    } else {
      partitionIdToMember
        .flatMap({case (pId, oldM) => {
          val newM = newPartitionTable(pId)
          if (oldM == newM) List() else List(Del(oldM, pId), Add(newM, pId))
        }})
        .toList
    }
  }
}
