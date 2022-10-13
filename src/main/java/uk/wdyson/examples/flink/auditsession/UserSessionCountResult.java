package uk.wdyson.examples.flink.auditsession;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class UserSessionCountResult {
    public String reqUser;
    public int count;
    public TimeWindow window;
}
