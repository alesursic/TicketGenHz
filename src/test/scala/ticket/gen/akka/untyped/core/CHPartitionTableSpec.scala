package ticket.gen.akka.untyped.core

import akka.actor.Address
import org.scalatest.flatspec.AnyFlatSpec
import ticket.gen.akka.untyped.core.ConsistentHashing.Table
import ticket.gen.akka.untyped.core.PartitionChange.*

class CHPartitionTableSpec extends AnyFlatSpec {
  "CH (i.e. cons. hash.) Partition Table" should "calculate rebalance actions when adding a member" in {
    //Prepare
    val addr0 = Address("", "", "127.0.0.1", 2551)
    val addr1 = Address("", "", "127.0.0.1", 2552)
    val addr2 = Address("", "", "127.0.0.1", 2553)

    val table: Table[Address, Int] = Map(
      addr0 -> List(3, 2, 1, 0),
      addr1 -> List(8, 7, 6, 5, 4)
    )

    val consistentHashing: ConsistentHashing[Address, Int] = ConsistentHashing(table)

    //Execute
    val result = consistentHashing.addMember(addr2)

    //Verify
    val expectedTable: Table[Address, Int] = Map(
      addr0 -> List(2, 1, 0),
      addr1 -> List(6, 5, 4),
      addr2 -> List(3, 7, 8)
    )
    val expectedActions: List[Change] = List(
      Del(addr1, 8), Add(addr2, 8), //migration of partition 8
      Del(addr0, 3), Add(addr2, 3), //migration of partition 3
      Del(addr1, 7), Add(addr2, 7)  //migration of partition 7
    )

    assert(result._1.getTable() === expectedTable)
    assert(result._2 === expectedActions)
  }
}
