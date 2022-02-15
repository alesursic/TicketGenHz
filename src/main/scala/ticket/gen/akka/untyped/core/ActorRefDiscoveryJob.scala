package ticket.gen.akka.untyped.core

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, ActorSelection, Address, Identify}
import akka.cluster.{Cluster, Member}
import ticket.gen.akka.untyped.core.ActorRefDiscoveryJob.{DiscoveryResult, IDENTIFY_ID}

import scala.collection.immutable
import scala.collection.mutable.Map

object ActorRefDiscoveryJob {
  val IDENTIFY_ID = 1
  case class DiscoveryResult(in: Map[Address, ActorRef])
}

class ActorRefDiscoveryJob(
  currMembers: immutable.Set[Member],
  replyTo: ActorRef
) extends Actor with ActorLogging {
  val members: Map[Address, ActorRef] = Map()
  var num = currMembers.size

  currMembers
      .map(member => remoteClusterEventActor(member.address))
      .foreach(_ ! Identify(IDENTIFY_ID))

  def receive = {
    case ActorIdentity(`IDENTIFY_ID`, Some(remoteClusterEventActor)) =>
      log.info("Actor {} identified", remoteClusterEventActor)
      members.put(remoteAddress(remoteClusterEventActor), remoteClusterEventActor)
      num = num - 1
      if (num == 0) {
        replyTo ! DiscoveryResult(members)
        context.stop(self)
      }
  }

  //Helpers:

  private[this] def remoteAddress(actorRef: ActorRef): Address =
    val address = actorRef.path.address
    if (address == context.self.path.address)
      Cluster(context.system).selfAddress
    else
      address

  private[this] def remoteClusterEventActor(address: Address): ActorSelection =
    context.actorSelection(
      String.format(
        "%s/user/cluster-event-actor",
        address
      )
    )
}
