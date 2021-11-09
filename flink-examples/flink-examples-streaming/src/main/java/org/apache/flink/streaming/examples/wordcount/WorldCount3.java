package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WorldCount3 {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境 flink run -d -t yarn-per-job     env =  StreamContextEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDSS = env.socketTextStream("localhost", 9999);

		FlatMapFunction flatMapFunction = new FlatMapFunction<String,String>() {
			@Override
			public void flatMap(String value, Collector<String> words) throws Exception {
				for (String word : value.split(" ")) {
					words.collect(word);
				}
			}
		};

		// 将Function 转换为 Operator 再转为 Transformation 添加到 List<Transformation<?>> transformations
		SingleOutputStreamOperator wordAndOne = lineDSS.flatMap(flatMapFunction);

		MapFunction mapFunction = new MapFunction<String,Tuple2<String,Long>>() {
			@Override
			public Tuple2<String,Long> map(String value) throws Exception {
				return Tuple2.of(value, 1L);
			}
		};
		SingleOutputStreamOperator<Tuple2<String,Long>> singleOutputStreamOperator = wordAndOne.map(mapFunction);

		KeySelector keySelector = new KeySelector<Tuple2<String,Long>, String>() {
			@Override
			public String getKey(Tuple2<String,Long> value) throws Exception {
				return value.f0;
			}
		};

		KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = singleOutputStreamOperator.keyBy(keySelector);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);
        // 6. 打印
        result.print();
        // 7. 执行
        env.execute();


    }
}
