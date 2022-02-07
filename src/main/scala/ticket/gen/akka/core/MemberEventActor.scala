package ticket.gen.akka.core

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import akka.cluster.ClusterEvent.{MemberEvent, MemberJoined, MemberLeft}
import akka.cluster.Member
import ticket.gen.akka.core.PartitionTable.{Add, Change, Del}
import ticket.gen.akka.setactors.SetActor.Command

//This actor detects joined (or up) members in the cluster and remotely deploys a single instance of set dispatcher actor
class MemberEventActor(context: ActorContext[MemberEvent]) extends AbstractBehavior[MemberEvent](context) {
  override def onMessage(msg: MemberEvent): Behavior[MemberEvent] = {
    msg match {
      case MemberJoined(member) =>
        //todo: remotely deploy SetDispatcher on the new member by using Group Router
        this
      case _ =>
        context.log.debug(msg.toString)
        this
    }
  }
}
