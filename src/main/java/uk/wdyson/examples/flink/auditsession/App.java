package uk.wdyson.examples.flink.auditsession;

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
    public static String PARAM_AUDIT_MIN_DATE = "audit.min_date";
    public static String PARAM_SESSION_DURATION_SECONDS = "session.duration";
    public static String PARAM_SESSION_OUTPUT = "session.output";
    public static String PARAM_KAFKA_PREFIX = "kafka.";

    public static DataStream<Audit> readAuditsFromFS(
            StreamExecutionEnvironment env,
            ParameterTool params) {

        String auditPath = params.getRequired(PARAM_AUDIT_FS_PATH);

        FileInputFormat<String> format = new TextInputFormat(new Path(auditPath));
        format.setNestedFileEnumeration(true);

        if (params.has(PARAM_AUDIT_MIN_DATE)) {
            format.setFilesFilter(new DateFileFilter(params.get(PARAM_AUDIT_MIN_DATE)));
        }

        int auditPollMs = params.getInt(PARAM_AUDIT_FS_POLL_SECONDS)*1000;

        DataStream<String> rawAudits = env
            .readFile(format, auditPath, FileProcessingMode.PROCESS_CONTINUOUSLY, auditPollMs)
            .uid("raw-audit-file-fs").name("Raw Audit Input from FS");

        DataStream<Audit> audits = rawAudits
            .map((rawAudit) -> Audit.fromJson(rawAudit))
            .uid("audit-input-fs-nullable").name("Audit Input from FS (Nullable)")
            .filter((audit) -> audit != null && audit.reqUser != null)
            .uid("audit-input-fs").name("Audit Input from FS");

        DataStream<Audit> auditsWithTime = audits
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Audit>forBoundedOutOfOrderness(Duration.ofDays(2))
                    .withTimestampAssigner((e, t) -> e.evtTime.getTime()))
            .uid("audit-input-fs-time").name("Audit Input from FS with Time");

        return auditsWithTime;
    }

    public static void printUserSessionDeniedAccessCountsToStdout(
            StreamExecutionEnvironment env,
            DataStream<UserSessionCountResult> stream) {

        stream
            .map((res) -> String.format("user='%s' denies=%d start=%d end=%d",
                res.reqUser,
                res.count,
                res.window.getStart(),
                res.window.getEnd()))
            .uid("stdout-format-stream").name("Format Stream")
            .print()
            .uid("stdout-session-sink").name("Output session counts to stdout");
    }

    public static void writeUserSessionDeniedAccessCountsToKafka(
            StreamExecutionEnvironment env,
            ParameterTool params,
            DataStream<UserSessionCountResult> stream) {

        Properties kafkaProps = readKafkaProperties(params);

        String bootstrapServers = kafkaProps.getProperty("bootstrap.servers");
        String topic = kafkaProps.getProperty("topic");

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(topic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setKafkaProducerConfig(kafkaProps)
            .build();

        stream
            .map((res) -> String.format("user='%s' denies=%d start=%d end=%d",
                res.reqUser,
                res.count,
                res.window.getStart(),
                res.window.getEnd()))
            .uid("kafka-format-stream").name("Format Stream")
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

    public static DataStream<UserSessionCountResult> extractDeniedAuditCountsUserSession(DataStream<Audit> audits, int sessionGapSeconds) {
        return audits
            .keyBy((audit) -> audit.reqUser)
            .window(EventTimeSessionWindows.withGap(Time.seconds(sessionGapSeconds)))
            .aggregate(new AggregateDeniedCounts(), new WrapUserAndWindowWithCount())
            .uid("user-session-denied-counts").name("Denied counts in User session")
            .filter((res) -> res.count != 0)
            .uid("user-session-denied-counts-non-zero").name("Non-zero denied counts in User session");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("A properties file must be provided as an argument");
        }

        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Audit> auditsWithTime = readAuditsFromFS(env, params);

        DataStream<UserSessionCountResult> userSessionDeniedAuditCounts =
            extractDeniedAuditCountsUserSession(auditsWithTime, params.getInt(PARAM_SESSION_DURATION_SECONDS));

        switch (params.get(PARAM_SESSION_OUTPUT, "kafka")) {
            case "kafka":
                writeUserSessionDeniedAccessCountsToKafka(env, params, userSessionDeniedAuditCounts);
                break;
            case "print":
                printUserSessionDeniedAccessCountsToStdout(env, userSessionDeniedAuditCounts);
                break;
            default:
                throw new IllegalArgumentException(String.format("Parameter %s must be one of: [kafka, print]", PARAM_SESSION_OUTPUT));
        }

        env.execute();
    }
}
