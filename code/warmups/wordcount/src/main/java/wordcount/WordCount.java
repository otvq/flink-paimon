package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * W1-2 断点首刀用 WordCount。
 * 断点位置见 BREAKPOINT 注释——先填完预测模板再打断点！
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.fromElements(
                "flink paimon flink",
                "paimon lakehouse flink",
                "deliberate practice flink");

        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                // BREAKPOINT-1（执行期）：在下面 reduce 的 lambda 体内打断点
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1));

        counts.print();

        // BREAKPOINT-2（提交期）：在下面这行打断点
        env.execute("W01 WordCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.toLowerCase().split("\\W+")) {
                if (!word.isEmpty()) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
