package ticket.gen.akka.untyped.core

import org.checkerframework.checker.units.qual.{A, m}
import ticket.gen.akka.untyped.core.ConsistentHashing.{MutTable, Table}
import ticket.gen.akka.untyped.core.PartitionChange.Change

import scala.collection.mutable

object ConsistentHashing {
  type Table[M, P] = Map[M, List[P]]
  type MutTable[M, P] = mutable.Map[M, List[P]]
  def apply[M, P](table: Table[M, P]): ConsistentHashing[M, P] = new ConsistentHashing(table)
}

class ConsistentHashing[M, P](table: Table[M, P]) {
  def getTable() = table

  def addMember(newM: M): (ConsistentHashing[M, P], List[Change]) = {
    val newTable: MutTable[M, P] = mutable.Map(table.toSeq: _*)
    newTable.put(newM, List())

    while(getMaxDiff(newTable) > 1) {                           //repeat until partitions are not evenly balanced
      getMax(newTable) match {                                  //find member with most partitions
        case (member, partitions) =>
          newTable.put(member, partitions.tail)                 //remove a partition from old member
          newTable.put(newM, partitions.head :: newTable(newM)) //add that partition to new member
      }
    }

    (ConsistentHashing(newTable.toMap), List()) //todo: change actions
  }

  def removeMember(oldM: M): (ConsistentHashing[M, P], List[Change]) = {
    val newTable: MutTable[M, P] = mutable.Map(table.toSeq: _*)
    val oldPartitions: List[P] = newTable.remove(oldM).get //!

    oldPartitions foreach {
      oldPartition => getMin(newTable) match {
        case (member, partitions) => newTable.put(member, oldPartition :: partitions)
      }
    }

    (ConsistentHashing(newTable.toMap), List()) //todo: change actions
  }

  //Helpers:

  //O(n^2)
  def getMaxDiff(table: MutTable[M, P]): Int = {
    //all possible pairs without duplicates: [x0, x1, x2] => [(x0, x1), (x0, x2), (x1, x2)]
    def crossProduct[A](xs: List[A]): List[(A, A)] = xs match {
      case Nil => List()
      case head::tail => xs.flatMap(x => tail.zip(List.fill(tail.size)(x))) ++ crossProduct(tail)
    }

    var max = 0
    val partitionCounts: List[Int] = table.values.map(_.size).toList

    crossProduct(partitionCounts) foreach {
      case (count0, count1) =>
        val diff = math.abs(count0 - count1)
        if (diff > max) { max = diff }
    }

    max
  }

  //O(n)
  def getMax(table: MutTable[M, P]): (M, List[P]) = {
    getMinOrMax(table, 0, (count, max) => count > max)
  }

  //O(n)
  def getMin(table: MutTable[M, P]): (M, List[P]) = {
    getMinOrMax(table, Int.MaxValue, (count, max) => count < max)
  }

  //O(n)
  def getMinOrMax(table: MutTable[M, P], minMax: Int, compare: (Int, Int) => Boolean): (M, List[P]) = {
    var newMinMax = minMax
    var result: (M, List[P]) = null

    table foreach {
      case (m, ps) =>
        val count = ps.size
        if (compare(count, newMinMax)) {
          newMinMax = count
          result = (m, ps)
        }
    }

    result
  }
}
