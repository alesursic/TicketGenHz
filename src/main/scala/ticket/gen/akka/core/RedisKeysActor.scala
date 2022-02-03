package ticket.gen.akka.core

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import ticket.gen.hz.state.RedisMarketKey

object RedisKeysActor {
  def apply(): Behavior[RedisMarketKey] =
    Behaviors.setup[RedisMarketKey](context => new RedisKeysActor(context))
}

class RedisKeysActor(context: ActorContext[RedisMarketKey]) extends AbstractBehavior[RedisMarketKey](context) {
  private var dirtyKeys = Set.empty[RedisMarketKey]

  override def onMessage(msg: RedisMarketKey): Behavior[RedisMarketKey] = {
    msg match {
      case _ => //every msg is RedisMarketKey so pattern matching isn't required
        context.log.info(msg.toString)
        dirtyKeys += msg
        this
    }
  }
}
