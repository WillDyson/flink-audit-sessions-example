import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import uk.wdyson.examples.flink.auditsession.App;
import uk.wdyson.examples.flink.auditsession.Audit;
import uk.wdyson.examples.flink.auditsession.UserSessionCountResult;

class TestAuditSession {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(1)
                .setNumberTaskManagers(1)
                .build());

    @Test
    void simpleAuditSession() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Audit audit1 = new Audit();
        audit1.reqUser = "wdyson";
        audit1.repo = "cm_kafka";
        audit1.result = 0;
        audit1.event_count = 10;
        audit1.evtTime = Timestamp.valueOf("2022-09-26 10:00:00");

        Audit audit2 = new Audit();
        audit2.reqUser = "wdyson";
        audit2.repo = "cm_hive";
        audit2.result = 1;
        audit2.event_count = 1;
        audit2.evtTime = Timestamp.valueOf("2022-09-26 10:10:00");

        Audit audit3 = new Audit();
        audit3.reqUser = "bob";
        audit3.repo = "cm_hive";
        audit3.result = 0;
        audit3.event_count = 1;
        audit3.evtTime = Timestamp.valueOf("2022-09-26 10:10:00");

        Audit audit4 = new Audit();
        audit4.reqUser = "bob";
        audit4.repo = "cm_hive";
        audit4.result = 0;
        audit4.event_count = 1;
        audit4.evtTime = Timestamp.valueOf("2022-09-26 10:20:00");

        DataStream<Audit> audits = env.fromElements(audit1, audit2, audit3, audit4)
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Audit>forMonotonousTimestamps()
                    .withTimestampAssigner((e, t) -> e.evtTime.getTime()));

        DataStream<UserSessionCountResult> results = App.extractDeniedAuditCountsUserSession(audits, 1200);

        CollectSink.values.clear();
        results.addSink(new CollectSink());

        env.execute();

        assertEquals(2, CollectSink.values.size());

        assertEquals("wdyson", CollectSink.values.get(0).reqUser);
        assertEquals(10, CollectSink.values.get(0).count);

        assertEquals("bob", CollectSink.values.get(1).reqUser);
        assertEquals(2, CollectSink.values.get(1).count);
    }

    private static class CollectSink implements SinkFunction<UserSessionCountResult> {
        public static final List<UserSessionCountResult> values =
            Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(UserSessionCountResult value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
