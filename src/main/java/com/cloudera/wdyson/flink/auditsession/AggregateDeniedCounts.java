package com.cloudera.wdyson.flink.auditsession;

import org.apache.flink.api.common.functions.AggregateFunction;

public class AggregateDeniedCounts implements AggregateFunction<Audit, Integer, Integer> {
    @Override
    public Integer add(Audit audit, Integer acc) {
        boolean allowed = audit.result == 1;

        if (!allowed) {
            return acc + 1;
        } else {
            return acc;
        }
    }

    @Override
    public Integer createAccumulator() {
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
