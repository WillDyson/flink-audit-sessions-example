package com.cloudera.wdyson.flink.auditsession;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;

public class App {
    public static String PARAM_AUDIT_PATH_S3 = "audit-path";
    public static String PARAM_SESSION_PATH_S3 = "session-path";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AuditInputFormat format = new AuditInputFormat(new Path(params.getRequired(PARAM_AUDIT_PATH_S3)));

        DataStream<Audit> audits = env
            .readFile(format, params.getRequired(PARAM_AUDIT_PATH_S3), FileProcessingMode.PROCESS_CONTINUOUSLY, Time.seconds(10).toMilliseconds())
            .flatMap(new FlatMapFunction<String, Audit>() {
                @Override
                public void flatMap(String json, Collector<Audit> out) {
                    Optional<Audit> maybeAudit = Audit.fromJson(json);

                    maybeAudit.ifPresent((audit) -> out.collect(audit));
                }
            })
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Audit>forBoundedOutOfOrderness(Duration.ofDays(5))
                    .withTimestampAssigner((e, t) -> e.evtTime.getTime()));

        AggregateFunction<Audit, Integer, Integer> deniedSum = new AggregateFunction<Audit, Integer, Integer>() {
            @Override
            public Integer add(Audit audit, Integer acc) {
                // result != 1 i.e. not allowed
                return acc + (audit.result != 1 ? 1 : 0);
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
        };

        ProcessWindowFunction<Integer, Tuple3<String, Long, Integer>, String, TimeWindow> bindWindowMetadata = new ProcessWindowFunction<Integer, Tuple3<String, Long, Integer>, String, TimeWindow>() {
            @Override
            public void process(String key,
                    ProcessWindowFunction<Integer, Tuple3<String, Long, Integer>, String, TimeWindow>.Context ctx,
                    Iterable<Integer> counts,
                    Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                Integer count = counts.iterator().next();
                out.collect(new Tuple3<String, Long, Integer>(key, ctx.window().getStart(), count));
            }

        };

        audits
            .filter((audit) -> audit.reqUser != null)
            .keyBy((audit) -> audit.reqUser)
            .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
            .aggregate(deniedSum, bindWindowMetadata)
            .filter((res) -> res.f2 != 0)
            .print();

        env.execute();
    }
}
