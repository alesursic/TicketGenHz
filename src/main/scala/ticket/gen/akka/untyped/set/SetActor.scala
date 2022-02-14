package ticket.gen.akka.untyped.set

import akka.actor.*
import ticket.gen.akka.untyped.set.SetActor._
import ticket.gen.hz.state.RedisMarketKey

object SetActor {
  sealed trait Command
  case class AddKey(key: RedisMarketKey) extends Command
  case class GetKeys(replyTo: ActorRef) extends Command
  case object PoissonPill extends Command
}

class SetActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case PoissonPill => context.stop(self)
  }
}
