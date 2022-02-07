package ticket.gen.akka.typed.core

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, GroupRouter, Routers}
import akka.cluster.ClusterEvent.{MemberEvent, MemberJoined, MemberLeft}
import akka.cluster.Member
import ticket.gen.akka.typed.core.PartitionTable.{Add, Change, Del}
import ticket.gen.akka.typed.setactors.SetDispatcher.{Command, SetDispatcherKey}

object MemberEventActor {
  def apply(): Behavior[MemberEvent] =
    Behaviors.setup[MemberEvent](context => {
      val group = Routers.group(SetDispatcherKey)
      new MemberEventActor(context, group)
    })
}

//This actor detects joined (or up) members in the cluster and remotely deploys a single instance of set dispatcher actor
class MemberEventActor(context: ActorContext[MemberEvent], group: GroupRouter[Command]) extends AbstractBehavior[MemberEvent](context) {
  override def onMessage(msg: MemberEvent): Behavior[MemberEvent] = {
    msg match {
      case MemberJoined(member) =>
        //todo: remotely deploy SetDispatcher on the new member by using Group Router
//        val router = context.spawn(group, "worker-group")
        this
      case _ =>
        context.log.debug(msg.toString)
        this
    }
  }
}
