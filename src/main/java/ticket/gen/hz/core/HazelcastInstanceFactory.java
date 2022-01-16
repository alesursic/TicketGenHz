package ticket.gen.hz.core;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;
import java.util.List;

public class HazelcastInstanceFactory {
    public static final List<String> config = Arrays.asList(
            "{develop:bk:0}",
            "{staging:bk:0}",
            "{develop:bk:24505}",
            "{develop:bk:24497}",
            "{staging:bk:24516}",
            "{develop:m:0}",
            "{staging:m:0}",
            "{staging:bk:26653}"
    );

    public static HazelcastInstance create() {
        Config hzConfig = new Config() ;
        hzConfig.setProperty("hazelcast.shutdownhook.enabled", "false");
        return Hazelcast.newHazelcastInstance(hzConfig);
    }
}
