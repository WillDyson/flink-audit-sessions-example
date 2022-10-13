package uk.wdyson.examples.flink.auditsession;

import org.apache.flink.api.common.functions.AggregateFunction;

public class AggregateDeniedCounts implements AggregateFunction<Audit, Integer, Integer> {
    @Override
    public Integer add(Audit audit, Integer acc) {
        // result == 1 means the action was allowed
        boolean allowed = audit.result == 1;

        // if not allowed then increment the count by the event count
        if (!allowed) {
            return acc + audit.event_count;
        } else {
            return acc;
        }
    }

    @Override
    public Integer createAccumulator() {
        // start count at 0
        return 0;
    }

    @Override
    public Integer getResult(Integer acc) {
        return acc;
    }

    @Override
    public Integer merge(Integer acc1, Integer acc2) {
        return acc1 + acc2;
    }
}
