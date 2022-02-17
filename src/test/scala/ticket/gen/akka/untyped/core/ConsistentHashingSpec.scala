package ticket.gen.akka.untyped.core

import org.scalatest.flatspec.AnyFlatSpec
import ticket.gen.akka.untyped.core.ConsistentHashing.Table

class ConsistentHashingSpec extends AnyFlatSpec {
  "Consistent Hashing" should "rebalance partitions when adding a member" in {
    val table: Table[Int, Int] = Map(
      0 -> List(3, 2, 1, 0),
      1 -> List(8, 7, 6, 5, 4)
    )
    val ch: ConsistentHashing[Int, Int] = ConsistentHashing(table)
    val newCh = ch.addMember(2)

    val expected = Map(
      0 -> List(2, 1, 0),
      1 -> List(6, 5, 4),
      2 -> List(7, 3, 8)
    )
    assert(newCh.getTable() === expected)
  }

  "Consistent Hashing" should "rebalance partitions when removing a member" in {
    val table: Table[Int, Int] = Map(
      0 -> List(2, 1, 0),
      1 -> List(6, 5, 4),
      2 -> List(7, 3, 8)
    )
    val ch: ConsistentHashing[Int, Int] = ConsistentHashing(table)
    val newCh = ch.removeMember(1)

    val expected = Map(
      0 -> List(4, 6, 2, 1, 0),
      2 -> List(5, 7, 3, 8)
    )
    assert(newCh.getTable() === expected)
  }

  "Consistent Hashing" should "rebalance each time after adding/removing members" in {
    val table: Table[Int, Int] = Map(0 -> (0 to 270).toList)
    val initCh = ConsistentHashing(table)
    val endCh = initCh
        .addMember(1)
        .addMember(2)
        .addMember(3)
        .removeMember(3)
        .removeMember(0)
        .addMember(3)

    val sizesActual = endCh.getTable().values.map(_.size).toList
    val sizesExpected = List(90, 91, 90)
    assert(sizesActual === sizesExpected)
  }
}
