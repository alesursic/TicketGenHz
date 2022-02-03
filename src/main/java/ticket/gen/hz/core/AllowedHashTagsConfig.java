package ticket.gen.hz.core;

import java.util.List;
import java.util.stream.Stream;
import java.util.Arrays;

public class AllowedHashTagsConfig {
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

    public static Stream<String> stream() {
        return config.stream();
    }
}
