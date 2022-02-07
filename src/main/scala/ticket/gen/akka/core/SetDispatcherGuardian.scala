package ticket.gen.akka.core

import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.cluster.ClusterEvent.MemberEvent
import ticket.gen.akka.core.PartitionTable.{Add, Change, Del}
import ticket.gen.akka.setactors.SetDispatcher
import ticket.gen.akka.setactors.SetDispatcher.{AddSetActor, RemoveSetActor, SetDispatcherKey}

object SetDispatcherGuardian {
  def apply(): Behavior[Receptionist.Listing] =
    Behaviors.setup[Receptionist.Listing](context => {
      context.system.receptionist ! Receptionist.Subscribe(SetDispatcherKey, context.self)
      context.spawnAnonymous(SetDispatcher())
      new SetDispatcherGuardian(context, PartitionTable.EMPTY)
    })
}

/*
 * Shard Coordinator (Singleton actor)
 *
 * This actor gets notified via the receptionist that SetDispatcher was deployed on a newly joined member
 * It then triggers rebalance of the cluster by first calculating the new partition table and then
 * sends commands to SetDispatchers on all members to either delete partition they own or to create a new one
 */
class SetDispatcherGuardian(
  context: ActorContext[Receptionist.Listing],
  var partitionTable: PartitionTable
) extends AbstractBehavior[Receptionist.Listing](context) {
  override def onMessage(msg: Receptionist.Listing): Behavior[Receptionist.Listing] = {
    msg match {
      case SetDispatcher.SetDispatcherKey.Listing(listings) =>
        val numOfNewMembers = listings.size
        val numOfOldMembers = partitionTable.numOfMembers()
        /*
         * listing either contains one actor more or one actor less than partition table
         * because an actor (or member) just left or joined the cluster
         */
        if (numOfNewMembers > numOfOldMembers) {
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
        } else if (numOfNewMembers < numOfOldMembers) {
          context.log.info("Partition rebalancing started due to a leaving member")
          //todo: implement
        }

        this
    }
  }
}
