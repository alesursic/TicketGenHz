package ticket.gen.akka.redis1;

import akka.actor.typed.ActorRef
import com.hazelcast.collection.ISet
import redis.clients.jedis.JedisPubSub
import ticket.gen.akka.setactors.SetDispatcher
import ticket.gen.hz.state.RedisMarketKey;

//Jedis 3rd party lib that just forwards all messages passed in via it's callback to main actor
class JedisPubSubImpl(redisKeysActor: ActorRef[SetDispatcher.Command]) extends JedisPubSub {
    private val KEYSPACE_PREFIX = "__keyspace@0__:";

    /**
     * On psubscribe for keyspace notifications
     *
     * @param pattern "__keyspace@0__:{develop:bk:0}*"
     * @param channel "__keyspace@0__:{develop:bk:0}:uof:1/sr:match:21796437/16/hcp=-2.25"
     * @param message "hset"
     */
    override def onPMessage(pattern: String, channel: String, message: String): Unit = {
        message match {
            case "hset" =>
                val changedKey: String = channel.split(KEYSPACE_PREFIX)(1);
                val msg = RedisMarketKey.parse(changedKey)
                redisKeysActor ! SetDispatcher.AddKey(msg)
        }
    }
}