package ticket.gen.akka.untyped.setactors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import ticket.gen.hz.state.RedisMarketKey
import ticket.gen.akka.untyped.setactors.SetDispatcher.*

object SetDispatcher {
  sealed trait Command
  case class AddKey(key: RedisMarketKey) extends Command
  case class AddSetActor(partitionId: Int) extends Command
  case class RemoveSetActor(partitionId: Int) extends Command
  case class GetKeys(replyTo: ActorRef) extends Command
}

class SetDispatcher extends Actor with ActorLogging {
  private var setActors = Map.empty[Int, ActorRef]

  override def preStart(): Unit = {
    log.info("Created and started set dispatcher {}", context.self)
  }

  override def receive = {
    case AddSetActor(pId) =>
      val setActor = context.actorOf(Props(classOf[SetActor]), "set-actor-" + pId)
      setActors += pId -> setActor
      log.info("Added set actor {}", setActor)

    case RemoveSetActor(pId) =>
      val setActor = setActors(pId)
      setActor ! SetActor.PoissonPill
      setActors -= pId
      log.info("Removed set actor {}", setActor)
  }
}
