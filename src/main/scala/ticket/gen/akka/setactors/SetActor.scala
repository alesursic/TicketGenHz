package ticket.gen.akka.setactors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import ticket.gen.akka.setactors.SetActor.*
import ticket.gen.hz.state.RedisMarketKey;

object SetActor {
  sealed trait Command
  case class AddKey(key: RedisMarketKey) extends Command
  case class GetKeys(replyTo: ActorRef[Set[RedisMarketKey]]) extends Command
  case object PoissonPill extends Command

  def apply(): Behavior[Command] =
    Behaviors.setup[Command](context => new SetActor(context))
}

class SetActor(context: ActorContext[Command]) extends AbstractBehavior[Command](context) {
  private var dirtyKeys = Set.empty[RedisMarketKey]

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case AddKey(key) =>
        dirtyKeys += key
        this
      case GetKeys(replyTo) =>
        replyTo ! dirtyKeys //todo: could empty the keys
        this
      case PoissonPill => Behaviors.stopped
    }
  }
}