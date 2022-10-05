import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;
import java.util.List;

import com.cloudera.wdyson.flink.auditsession.App;
import com.cloudera.wdyson.flink.auditsession.Audit;
import com.cloudera.wdyson.flink.auditsession.WrapValueWithKeyAndWindow;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class TestAuditSession {
    @Test
    void simpleAuditSession() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("jobmanager.bind-host", "127.0.0.1");
        conf.setString("taskmanager.bind-host", "127.0.0.1");
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .createLocalEnvironment(1, conf);

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

        List<WrapValueWithKeyAndWindow<String, Integer>.ValueWithKeyAndWindow> output = results.executeAndCollect(100);

        assertEquals(2, output.size());

        assertEquals("wdyson", output.get(0).key);
        assertEquals(1, output.get(0).value);

        assertEquals("bob", output.get(1).key);
        assertEquals(2, output.get(1).value);
    }
}
