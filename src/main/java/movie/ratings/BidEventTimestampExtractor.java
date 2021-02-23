package movie.ratings;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class BidEventTimestampExtractor extends AscendingTimestampExtractor<Ysb.BidEvent> {
    @Override
    public long extractAscendingTimestamp(Ysb.BidEvent element) {
        return element.geteventTime();
    }
}
