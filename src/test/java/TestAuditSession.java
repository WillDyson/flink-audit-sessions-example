import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.cloudera.wdyson.flink.auditsession.App;
import com.cloudera.wdyson.flink.auditsession.Audit;
import com.cloudera.wdyson.flink.auditsession.WrapValueWithKeyAndWindow;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

class TestAuditSession {
    private static Map<String, Integer> lastSessionResult = new ConcurrentHashMap<>();
    private static Map<String, Integer> sessionCounts = new ConcurrentHashMap<>();

    @Test
    void simpleAuditSession() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Audit audit1 = new Audit();
        audit1.reqUser = "wdyson";
        audit1.result = 0;
        audit1.evtTime = Timestamp.valueOf("2022-09-26 10:00:00");

        Audit audit2 = new Audit();
        audit2.reqUser = "wdyson";
        audit2.result = 1;
        audit2.evtTime = Timestamp.valueOf("2022-09-26 10:10:00");

        Audit audit3 = new Audit();
        audit3.reqUser = "bob";
        audit3.result = 0;
        audit3.evtTime = Timestamp.valueOf("2022-09-26 10:10:00");

        Audit audit4 = new Audit();
        audit4.reqUser = "bob";
        audit4.result = 0;
        audit4.evtTime = Timestamp.valueOf("2022-09-26 10:20:00");

        DataStream<Audit> audits = env.fromElements(audit1, audit2, audit3, audit4)
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Audit>forMonotonousTimestamps()
                    .withTimestampAssigner((e, t) -> e.evtTime.getTime()));

        DataStream<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> results = App.extractDeniedAuditCountsUserSession(audits, 1200);

        results.addSink(new SinkFunction<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow>() {
            @Override
            public void invoke(WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow value, Context context) {
                lastSessionResult.put(value.key, value.value);
                sessionCounts.put(value.key, sessionCounts.getOrDefault(value.key, 0) + 1);
            }
        })
        .setParallelism(1);

        env.execute();

        assertEquals(2, lastSessionResult.size());
        assertEquals(2, sessionCounts.size());

        if (!lastSessionResult.containsKey("wdyson")
                || !sessionCounts.containsKey("wdyson")
                || !lastSessionResult.containsKey("bob")
                || !sessionCounts.containsKey("bob")) {
            fail("Session tracker did not contain expected keys");
        }

        assertEquals(1, sessionCounts.get("wdyson"));
        assertEquals(1, sessionCounts.get("bob"));

        assertEquals(1, lastSessionResult.get("wdyson"));
        assertEquals(2, lastSessionResult.get("bob"));
    }
}
