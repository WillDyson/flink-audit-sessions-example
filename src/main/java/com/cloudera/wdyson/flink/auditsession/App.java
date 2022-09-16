package com.cloudera.wdyson.flink.auditsession;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.Map.Entry;

public class App {
    public static String PARAM_AUDIT_FS_PATH = "audit.path";
    public static String PARAM_AUDIT_FS_POLL_SECONDS = "audit.poll";
    public static String PARAM_SESSION_DURATION_SECONDS = "session.duration";
    public static String PARAM_KAFKA_PREFIX = "kafka.";

    public static DataStream<Audit> readAuditsFromFS(
            StreamExecutionEnvironment env,
            String path,
            int fsPollSeconds) {

        AuditInputFormat format = new AuditInputFormat(new Path(path));

        return env
            .readFile(format, path, FileProcessingMode.PROCESS_CONTINUOUSLY, Time.seconds(fsPollSeconds).toMilliseconds())
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
    }

    public static void writeUserSessionDeniedAccessCountsToKafka(
            StreamExecutionEnvironment env,
            Properties kafkaProps,
            DataStream<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> stream) {

        stream.print();
    }

    private static Properties readKafkaProperties(ParameterTool params) {
        Properties props = new Properties();

        for (Entry<String, String> param : params.toMap().entrySet()) {
            if (param.getKey().startsWith(PARAM_KAFKA_PREFIX)) {
                String strippedKey = param.getKey().substring(PARAM_KAFKA_PREFIX.length());

                props.setProperty(strippedKey, param.getValue());
            }
        }

        return props;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("A properties file must be provided as an argument");
        }

        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Audit> audits = readAuditsFromFS(env, params.getRequired(PARAM_AUDIT_FS_PATH), params.getInt(PARAM_AUDIT_FS_POLL_SECONDS));

        DataStream<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> userSessionDeniedAuditCounts = audits
            .filter((audit) -> audit.reqUser != null)
            .keyBy((audit) -> audit.reqUser)
            .window(EventTimeSessionWindows.withGap(Time.minutes(params.getInt(PARAM_SESSION_DURATION_SECONDS))))
            .aggregate(new AggregateDeniedCounts(), new WrapValueWithKeyAndWindow<String, Integer>())
            .filter((res) -> res.value != 0);

        Properties kafkaProps = readKafkaProperties(params);

        writeUserSessionDeniedAccessCountsToKafka(env, kafkaProps, userSessionDeniedAuditCounts);

        env.execute();
    }
}
