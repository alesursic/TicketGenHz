package ticket.gen.akka.untyped.setactors

import akka.actor.*
import ticket.gen.akka.untyped.setactors.SetActor._
import ticket.gen.hz.state.RedisMarketKey

object SetActor {
  sealed trait Command
  case class AddKey(key: RedisMarketKey) extends Command
  case class GetKeys(replyTo: ActorRef) extends Command
  case object PoissonPill extends Command
}

class SetActor extends Actor with ActorLogging {
  override def preStart(): Unit = {
    log.info("Created and started set actor {}", context.self)
  }

  override def receive: Receive = {
    case PoissonPill => context.stop(self)
  }
}
