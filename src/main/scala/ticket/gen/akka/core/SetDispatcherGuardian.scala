package ticket.gen.akka.core

import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.cluster.ClusterEvent.MemberEvent
import ticket.gen.akka.core.PartitionTable2.{Add, Change, Del}
import ticket.gen.akka.setactors.SetDispatcher
import ticket.gen.akka.setactors.SetDispatcher.{AddSetActor, RemoveSetActor, SetDispatcherKey}

//todo: use ClusterSingleton
object SetDispatcherGuardian {
  def apply(): Behavior[Receptionist.Listing] =
    Behaviors.setup[Receptionist.Listing](context => {
      context.system.receptionist ! Receptionist.Subscribe(SetDispatcherKey, context.self)
      context.spawnAnonymous(SetDispatcher()) //can there be a race condition (guardian not listening yet)

      new SetDispatcherGuardian(context, PartitionTable2.EMPTY)
    })
}

class SetDispatcherGuardian(
  context: ActorContext[Receptionist.Listing],
  var partitionTable: PartitionTable2
) extends AbstractBehavior[Receptionist.Listing](context) {
  override def onMessage(msg: Receptionist.Listing): Behavior[Receptionist.Listing] = {
    msg match {
      case SetDispatcher.SetDispatcherKey.Listing(listings) =>
        /*
         * listing either contains one actor more or one actor less than partition table
         * because an actor (or member) just left or joined the cluster
         */
        if (listings.size > partitionTable.numOfMembers()) {
          context.log.info("Partition rebalancing started due to a joining member")
          val joinedMember = listings.find(!partitionTable.containsMember(_)).get

          val newPartitionTable = partitionTable.addMemberAndRebalance(joinedMember)
          val changes: List[Change] = partitionTable.diff(newPartitionTable)
          partitionTable = newPartitionTable

          //NOTE: It may also send a message to itself (this member's set dispatcher)
          changes foreach (change => change match {
            case Add(m, pId) => {
              m ! AddSetActor(pId)
            }
            case Del(m, pId) => {
              m ! RemoveSetActor(pId)
            }
          })
        } else {
          context.log.info("Partition rebalancing started due to a leaving member")
          //todo: implement
        }

        this
    }
  }
}
