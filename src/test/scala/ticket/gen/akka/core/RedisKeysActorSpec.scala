package ticket.gen.akka.core

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import ticket.gen.hz.state.RedisMarketKey

class RedisKeysActorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  "Redis keys actor" must {
    "print received redis keys" in {
      val redisKeysActor = spawn(RedisKeysActor())

      redisKeysActor ! RedisMarketKey.parse("{staging:bk:22017}:uof:1/sr:match:21796437/16/hcp=-2.25")

    }
  }
}
