package uk.wdyson.examples.flink.auditsession;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WrapUserAndWindowWithCount extends ProcessWindowFunction<Integer, UserSessionCountResult, String, TimeWindow> {
    @Override
    public void process(String reqUser,
            ProcessWindowFunction<Integer, UserSessionCountResult, String, TimeWindow>.Context ctx,
            Iterable<Integer> counts, Collector<UserSessionCountResult> out) throws Exception {

        UserSessionCountResult result = new UserSessionCountResult();

        result.reqUser = reqUser;
        result.count = counts.iterator().next();
        result.window = ctx.window();

        out.collect(result);
    }
}
