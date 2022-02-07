package com.example.routing

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}
import akka.cluster.typed.Cluster
import com.example.routing.Worker.workerServiceKey

object GroupReceiverWorkerMain {
  def main(args: Array[String]): Unit = {
    val behavior = Behaviors.setup[Unit] { ctx =>
      val worker = ctx.spawn(Worker(), "worker")
      ctx.system.receptionist ! Receptionist.Register(workerServiceKey, worker)

      Behaviors.empty
    }

    val sys = ActorSystem(behavior, "RedisKeysSys")
    Cluster(sys)
  }
}

