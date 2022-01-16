package ticket.gen.hz;

import fj.P;
import fj.P2;
import fj.data.Stream;
import ticket.gen.hz.core.HazelcastInstanceFactory;

import java.util.Random;

public class RedisMarketKeyGenerator {
    private static final String[] HASH_TAGS = HazelcastInstanceFactory.config.toArray(String[]::new);
    private static final String[] SPECIFIERS = new String[]{"hcp=-1.25", "hcp=-2.25", "hcp=-3.25", "hcp=-4.25"};

    private final Random rng;

    public RedisMarketKeyGenerator(Random rng) {
        this.rng = rng;
    }

    public Stream<P2<Integer, String>> stream() {
        return Stream.fromFunction(idx -> {
            String hashTag = HASH_TAGS[rng.nextInt(HASH_TAGS.length)];
            int producerId = rng.nextInt(15);
            int matchId = Math.abs(rng.nextInt());
            int marketId = rng.nextInt(255);
            String specifier = SPECIFIERS[rng.nextInt(SPECIFIERS.length)];

            String rawKey = String.format(
                    "%s:uof:%d/sr:match:%d/%d/%s",
                    hashTag,
                    producerId,
                    matchId,
                    marketId,
                    specifier
            );

            return P.p(idx.intValue(), rawKey);
        });
    }
}
