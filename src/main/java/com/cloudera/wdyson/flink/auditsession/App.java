package com.cloudera.wdyson.flink.auditsession;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;
import java.util.Map.Entry;

public class App {
    public static String PARAM_AUDIT_FS_PATH = "audit.path";
    public static String PARAM_AUDIT_FS_POLL_SECONDS = "audit.poll";
    public static String PARAM_AUDIT_ALLOWED_LATENESS_DAYS = "audit.allowed_lateness";
    public static String PARAM_SESSION_DURATION_SECONDS = "session.duration";
    public static String PARAM_SESSION_OUTPUT = "session.output";
    public static String PARAM_KAFKA_PREFIX = "kafka.";

    public static DataStream<Audit> readAuditsFromFS(
            StreamExecutionEnvironment env,
            ParameterTool params) {

        String path = params.getRequired(PARAM_AUDIT_FS_PATH);

        FileInputFormat<String> format = new TextInputFormat(new Path(path));

        format.setNestedFileEnumeration(true);

        return env
            .readFile(
                    format,
                    path,
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    Time.seconds(params.getInt(PARAM_AUDIT_FS_POLL_SECONDS)).toMilliseconds())
            .uid("raw-audit-file-fs").name("Raw Audit Input from FS")
            .flatMap(new FlatMapAuditsFromJson())
            .uid("audit-input-fs").name("Audit Input from FS")
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Audit>forBoundedOutOfOrderness(Duration.ofDays(params.getInt(PARAM_AUDIT_ALLOWED_LATENESS_DAYS, 2)))
                    .withTimestampAssigner((e, t) -> e.evtTime.getTime()))
            .uid("audit-input-fs-time").name("Audit Input from FS with Time");
    }

    public static void printUserSessionDeniedAccessCountsToStdout(
            StreamExecutionEnvironment env,
            DataStream<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> stream) {

        stream
            .map((res) -> String.format(
                "%d-%d: %s -> %d",
                res.window.getStart(),
                res.window.getEnd(),
                res.key,
                res.value))
            .print()
            .uid("stdout-session-sink").name("Output session counts to stdout");
    }

    public static void writeUserSessionDeniedAccessCountsToKafka(
            StreamExecutionEnvironment env,
            ParameterTool params,
            DataStream<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> stream) {

        Properties kafkaProps = readKafkaProperties(params);

        String bootstrapServers = kafkaProps.getProperty("bootstrap.servers");
        String topic = kafkaProps.getProperty("topic");

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic(topic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setKafkaProducerConfig(kafkaProps)
            .build();

        stream
            .map((res) -> String.format(
                "%d-%d: %s -> %d",
                res.window.getStart(),
                res.window.getEnd(),
                res.key,
                res.value))
            .sinkTo(sink)
            .uid("kafka-session-sink").name("Output session counts to Kafka");
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

        DataStream<Audit> audits = readAuditsFromFS(env, params);

        DataStream<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> userSessionDeniedAuditCounts = audits
            .filter((audit) -> audit.reqUser != null)
            .uid("audits-non-null").name("Audits with non-null requestor")
            .keyBy((audit) -> audit.reqUser)
            .window(EventTimeSessionWindows.withGap(Time.minutes(params.getInt(PARAM_SESSION_DURATION_SECONDS))))
            .aggregate(new AggregateDeniedCounts(), new WrapValueWithKeyAndWindow<String, Integer>())
            .uid("user-session-denied-counts").name("Denied counts in User session")
            .filter((res) -> res.value != 0)
            .uid("user-session-denied-counts-non-zero").name("Non-zero denied counts in User session");

        String sessionOutputType = params.get(PARAM_SESSION_OUTPUT, "kafka");

        if (sessionOutputType.equals("kafka")) {
            writeUserSessionDeniedAccessCountsToKafka(env, params, userSessionDeniedAuditCounts);
        } else if (sessionOutputType.equals("print")) {
            printUserSessionDeniedAccessCountsToStdout(env, userSessionDeniedAuditCounts);
        } else {
            throw new IllegalArgumentException(String.format("Parameter %s must be one of: [kafka, print]", PARAM_SESSION_OUTPUT));
        }

        env.execute();
    }
}
