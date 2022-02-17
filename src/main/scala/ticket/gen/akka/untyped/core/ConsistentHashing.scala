package ticket.gen.akka.untyped.core

import scala.collection.mutable

object ConsistentHashing {
  def apply[M, P](table: Map[M, List[P]]): ConsistentHashing[M, P] = new ConsistentHashing(table)
}

class ConsistentHashing[M, P](table: Map[M, List[P]]) {
  def getTable() = table

  def addMember(newM: M): ConsistentHashing[M, P] = {
    val newTable: mutable.Map[M, List[P]] = mutable.Map(table.toSeq: _*)
    newTable.put(newM, List())

    val max = getMax()
    val m = max._1
    val p = max._2.head

    newTable.put(m, newTable(m).tail)
    newTable.put(newM, p :: newTable(newM))

    ConsistentHashing(newTable.toMap)
  }

  private[this] def getMax(): (M, List[P]) = {
    var max = 0
    var result: (M, List[P]) = null

    table foreach {
      case (m, ps) => {
        val count = ps.size
        if (count > max) {
          max = count
          result = (m, ps)
        }
      }
    }

    result
  }
}
