package ticket.gen.akka.untyped.core

import org.scalatest.flatspec.AnyFlatSpec

class ConsistentHashingSpec extends AnyFlatSpec {
  "Consistent Hashing" should "rebalance partitions when adding a second member" in {
    val table: Map[Int, List[Int]] = Map(0 -> List(1, 0))
    val ch: ConsistentHashing[Int, Int] = ConsistentHashing(table)
    val newCh = ch.addMember(1)

    val expected = Map(0 -> List(0), 1 -> List(1))
    assert(newCh.getTable() === expected)
  }
}
