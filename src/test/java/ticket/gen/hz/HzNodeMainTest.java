package ticket.gen.hz;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Random;

public class HzNodeMainTest {
    @Test
    public void addingHzNodeCausesRedisDisconnectsOnOtherHzNodes() {
        //Prepare

        /*
         * Before starting up the Hz cluster:
         *  1. Find out hash slots for given hash tags from config server (8 of them)
         *  2. Find out to which Redis nodes they belong
         *  3. Find out to which Hz partitions they belong to
         */

//        Which hash slot belongs to which hz partition:
//         96: [4749]
//         240: [7484]
//         180: [15060]
//         100: [4943]
//         134: [3377]
//         72: [4202]
//         269: [12502]
//         14: [9539]

        /*
         * Startup Hz cluster with 2 Hz nodes:
         *  1. Find out which partitions belong to which hz node
         *  2. Find out to which Redis nodes each hz node is connected to
         */

//        Which partition belongs to which hz node:
//         HZ-5701: [240, 180, 269]
//         HZ-5702: [96, 100, 134, 72, 14]

        //Execute

        /*
         * Add a new (third) Hz node
         *  1. check Redis disconnect events on both Hz nodes
         */

        //HZ-5701: disconnecting from RedisNode 15060..
        //HZ-5702: disconnecting from RedisNode 9539..

        //Verify

        /*
         * 1. check that the nodes that had the above Redis disconnects no longer have these Redis connections
         * 2. check that the new node has the connections replacing the disconnects from the other two nodes
         */

//        Expected:
//         HZ-5701: [{240: [7484]}, {269: [12502]}]
//         HZ-5702: [{96: [4749]}, {100: [4943]}, {134: [3377]}, {72: [4202]}]
//         HZ-5703: [{14: [9539]}, {180: [15060]}]

//        Actual:
//         HZ-5701: [{240: [7484]}, {269: [12502]}]
//         HZ-5702: [{96: [4749]}, {100: [4943]}, {134: [3377]}, {72: [4202]}, ]
//         HZ-5703: [{180: [15060]}, {14: [9539]}]

        //PASS
    }

    @Test
    public void removingHzNodeCausesRedisConnectionsOnOtherHzNodes() {
        //Prepare

//         HZ-5701: [{240: [7484]}, {269: [12502]}]
//         HZ-5702: [{96: [4749]}, {100: [4943]}, {134: [3377]}, {72: [4202]}, ]
//         HZ-5703: [{180: [15060]}, {14: [9539]}]

        //Execute

        //HZ-5701:
//        connecting to RedisNode 4749..
//        connecting to RedisNode 4943..
//        connecting to RedisNode 3377..
        //HZ-5702:
//        connecting to RedisNode 4202..

        //Verify

        //HZ-5701: [{96: [4749]}, {240: [7484]}, {100: [4943]}, {134: [3377]}, {269: [12502]}]
        //HZ-5703: [{180: [15060]}, {72: [4202]}, {14: [9539]}]

        //PASS
    }

    /*
     * This is the load test of the application.
     * It's writing keys into redis cluster and user must be (manually) restarting various hazelcast nodes
     * one by one (2 out of 3 nodes must be alive at all times).
     * Verification is done by sending "partitions" command on hazelcast management tool
     * in order to determine the number of keys in hazelcast cluster.
     * The loss should be less than 0.1%.
     */
    @Test
    public void loadTest() {
        JedisCluster jedisCluster = new JedisCluster(new HostAndPort("localhost", 7000));
        RedisMarketKeyGenerator keyGenerator = new RedisMarketKeyGenerator(new Random(System.currentTimeMillis()));

        keyGenerator
                .stream()
                .take(20000)
                .foreachDoEffect(pair -> {
                    String key = pair._2();
                    jedisCluster.hmset(key, ImmutableMap.of("k0", "v0"));

                    int idx = pair._1();
                    try {
                        if ((idx + 1) % 500 == 0) {
                            Thread.sleep(2000);
                        } else {
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e); //early termination
                    }
                });
    }
}
