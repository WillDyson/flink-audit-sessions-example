package com.cloudera.wdyson.flink.auditsession;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class FlatMapAuditsFromJson implements FlatMapFunction<String, Audit> {
    @Override
    public void flatMap(String json, Collector<Audit> out) throws Exception {
        Audit audit = Audit.fromJson(json);

        if (audit != null) {
            out.collect(audit);
        }
    }
}
