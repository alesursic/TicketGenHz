package ticket.gen.hz.core;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;
import java.util.List;

public class HazelcastInstanceFactory {
    public static HazelcastInstance create() {
        Config hzConfig = new Config() ;
        hzConfig.setProperty("hazelcast.shutdownhook.enabled", "false");
        return Hazelcast.newHazelcastInstance(hzConfig);
    }
}
