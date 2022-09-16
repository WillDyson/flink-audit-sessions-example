package com.cloudera.wdyson.flink.auditsession;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WrapValueWithKeyAndWindow<K, V> extends ProcessWindowFunction<V, WrapValueWithKeyAndWindow<K, V>.ValueWithKeyAndWindow, K, TimeWindow> {
    public class ValueWithKeyAndWindow {
        public K key;
        public V value;
        public TimeWindow window;
    }

    @Override
    public void process(K key,
            ProcessWindowFunction<V, ValueWithKeyAndWindow, K, TimeWindow>.Context ctx,
            Iterable<V> values, Collector<ValueWithKeyAndWindow> out) throws Exception {

        ValueWithKeyAndWindow data = new ValueWithKeyAndWindow();

        data.key = key;
        data.value = values.iterator().next();
        data.window = ctx.window();

        out.collect(data);
    }
}
