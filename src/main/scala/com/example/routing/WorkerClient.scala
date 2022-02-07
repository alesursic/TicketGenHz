package com.example.routing

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}
import akka.cluster.typed.Cluster
import com.example.routing.Worker.workerServiceKey

object GroupWorkerMain {
  def main(args: Array[String]): Unit = {
    val behavior = Behaviors.setup[Unit] { ctx =>
      val group = Routers.group(workerServiceKey)
      val router = ctx.spawn(group, "worker-group")

      (0 to 10).foreach { n =>
        router ! Worker.DoLog(s"msg $n")
      }

      Behaviors.empty
    }

    val sys = ActorSystem(behavior, "RedisKeysSys")
    Cluster(sys)
  }
}

