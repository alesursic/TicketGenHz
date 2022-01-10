<h1>Ticket Generator Hazelcast</h1>

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
