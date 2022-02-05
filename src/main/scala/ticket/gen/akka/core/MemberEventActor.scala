package ticket.gen.akka.core

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import akka.cluster.ClusterEvent.{MemberEvent, MemberJoined, MemberLeft}
import akka.cluster.Member
import ticket.gen.akka.core.PartitionTable.{Add, Change, Del}
import ticket.gen.akka.setactors.SetActor.Command

class MemberEventActor(
  context: ActorContext[MemberEvent],
  var partitionTable: PartitionTable
) extends AbstractBehavior[MemberEvent](context) {
  override def onMessage(msg: MemberEvent): Behavior[MemberEvent] = {
    msg match {
      case MemberJoined(member) =>
        val newPartitionTable = partitionTable.addMemberAndRebalance(member)
        val changes: List[Change] = partitionTable.diff(newPartitionTable)
        partitionTable = newPartitionTable

        changes foreach (change => change match {
          case Add(m, pId) => {
            val dest = findActor(m)
            //send AddSetActor(pId) to that actor
          }
          case Del(m, pId) => {
            val dest = findActor(m)
            //send RemoveSetActor(pId) to that actor
          }
        })
        this
      case MemberLeft(member) =>
        partitionTable = partitionTable.removeMemberAndRebalance(member)
        this
    }
  }

  //Helpers:

  def findActor(m: Member): ActorRef[Command] = ???
}
