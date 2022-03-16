package ticket.gen.akka.untyped.core

import akka.actor.Address

object PartitionChange {
  sealed trait Change(m: Address) {
    def getAddress() = m
  }
  case class Add(m: Address, pId: Int) extends Change(m)
  case class Del(m: Address, pId: Int) extends Change(m)
}
