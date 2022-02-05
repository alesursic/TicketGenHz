package ticket.gen.akka.setactors

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import ticket.gen.hz.state.RedisMarketKey
import ticket.gen.akka.setactors.SetDispatcher.{AddKey, AddSetActor, Command, GetKeys, RemoveSetActor}
import ticket.gen.hz.redis.HashTag

object SetDispatcher {
  val SetDispatcherKey = ServiceKey[Command]("setDispatcher")

  sealed trait Command
  case class AddKey(key: RedisMarketKey) extends Command
  case class AddSetActor(partitionId: Int) extends Command
  case class RemoveSetActor(partitionId: Int) extends Command
  case class GetKeys(replyTo: ActorRef[Set[RedisMarketKey]]) extends Command

  def apply(): Behavior[Command] =
    Behaviors.setup[Command](context => {
      context.system.receptionist ! Receptionist.Register(SetDispatcherKey, context.self)
      new SetDispatcher(context)
    })
}

class SetDispatcher(context: ActorContext[Command]) extends AbstractBehavior[Command](context) {
  private var setActors = Map.empty[Int, ActorRef[SetActor.Command]]

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case AddKey(key) =>
        val hashTag = HashTag.fromString(key.toString)
        setActors(hashTag.hashSlot()) ! SetActor.AddKey(key)
        this
      case GetKeys(replyTo) =>
        setActors.values foreach { setActor =>
          setActor ! SetActor.GetKeys(replyTo)
        }
        this
      case AddSetActor(partitionId) =>
        val setActor = context.spawn(SetActor(), "set-actor-" + partitionId)
        setActors += partitionId -> setActor
        context.log.info("Added set actor {}", setActor)
        //get hash-slot via partition-id mapping
        //connect to redis pubsub for that hash-slot
        this
      case RemoveSetActor(partitionId) =>
        val setActor = setActors(partitionId)
        setActor ! SetActor.PoissonPill
        setActors -= partitionId
        context.log.info("Removed set actor {}", setActor)
        this
    }
  }
}