import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.cloudera.wdyson.flink.auditsession.App;
import com.cloudera.wdyson.flink.auditsession.Audit;
import com.cloudera.wdyson.flink.auditsession.WrapValueWithKeyAndWindow;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

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

        CollectSink.values.clear();
        results.addSink(new CollectSink());

        env.execute();

        assertEquals(2, CollectSink.values.size());

        assertEquals("wdyson", CollectSink.values.get(0).key);
        assertEquals(1, CollectSink.values.get(0).value);

        assertEquals("bob", CollectSink.values.get(1).key);
        assertEquals(2, CollectSink.values.get(1).value);
    }

    private static class CollectSink implements SinkFunction<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> {
        public static final List<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> values =
            Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
