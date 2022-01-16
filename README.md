<h1>Ticket Generator Hazelcast</h1>

<h2>Intro</h2>

<p>Ticket generator rewrite into Hazelcast cluster.</p> 
<p>Redis keys (dirty keys) representing status data and meta data is a distributed in-mem state of the cluster. 
Redis values for these keys are not a part of the in-mem state.</p>

<p>
Each hz partition will contain one or more Redis hash slots and each hash slot will contain
multiple Redis keys from keyspace notifications.
</p>
<p>
partiton = [hash_slot] <br>
hash_slot = [redis_key] <br> <br>
{ <br>
  &nbsp"partition_0": { <br>
  &nbsp&nbsp  "hash_slot_64238": [key0, key1, ..], <br>
  &nbsp&nbsp  "hash_slot_423678": [key0', key1', ..] <br>
  &nbsp}, <br>
  &nbsp"partition_32": { <br>
  &nbsp&nbsp  "hash_slot_423": [key0'', key1'', ..] <br>
  &nbsp} <br>
} <br>
</p>
<p>
It's important to note that a hash slot cannot be a member of more than one partition and that
each redis key cannot be a member of more than one hash slot.
</p>

<h2>The way it works</h2>
<p>
Hazelcast member (node) may either be joining the cluster or leaving the cluster (gracefully or 
due to a sigkill). When member is joining the cluster it's not yet subscribed to partition migration events
and it receives partitions that were previously owned by other alive members during the initialization.
Therefore it cannot connect to Redis nodes on partition migration event. It connects after hazelcast 
initialization completes. Because other members do get notified about loosing their partitions they should
not disconnect from redis because this would cause temporary disconnect from redis until the joining member
connects (0 connections). Instead it remembers partitions it used to own.
</p>
<p>
When joining member starts connecting to redis it sends a disconnect pubsub message containing partitionId.
Members that were previous owners of this partition simply disconnect from redis for that partition. Each time
it sends a disconnect message it sleeps for a short time to make sure connection was firmly established (for that 
time there are 2 duplicate connections).
</p>
<p>
When member is leaving the cluster partition migration event handlers are invoked on other members
and they create connections for these partitions. In the case of graceful shutdown the member does not
disconnect in the migration event handler and there are 2 duplicate connections until
leaving member terminates and disconnects from redis.
</p>

<h2>How to test</h2>
<p>
Setup a minimal Redis cluster for instance with 3 master nodes and 0 replicas.
Start a Hazelcast cluster with 3 nodes and run the load test in the test directory.
Start and stop hazelcast nodes while the load test is running - desirably with sigint 
because this is the behaviour in deployment of the cluster.
Check the number of keys after the load test completes. The loss of data should be 
less than 0.1% if sigint was used each time.
</p>