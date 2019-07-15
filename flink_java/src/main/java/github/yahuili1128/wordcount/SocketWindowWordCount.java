package github.yahuili1128.wordcount;

import github.yahuili1128.pojo.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description : java flink socket wordcount
 * @Author : LiYahui
 * @Date : 2019-06-25 16:37
 * @Version : V1.0
 */
public class SocketWindowWordCount {
	public static void main(String[] args) throws Exception {
		//	the port to connect to
		final int port;
		try {
			ParameterTool parameterTool = ParameterTool.fromArgs(args);
			port = parameterTool.getInt("port");
		} catch (Exception e) {
			System.out.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
			return;
		}
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> text = env.socketTextStream("liyahui-01", port, "\n");


		SingleOutputStreamOperator<WordCount> wordcounts = text.flatMap(new FlatMapFunction<String, WordCount>() {
			@Override
			public void flatMap(String value, Collector<WordCount> out) throws Exception {
				for (String word : value.split("\\s")) {
					out.collect(new WordCount(word, 1L));
				}
			}
		}).keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).reduce(new ReduceFunction<WordCount>() {
			@Override
			public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
				return new WordCount(value1.word, value1.count + value2.count);
			}
		});
		wordcounts.print().setParallelism(1);

		env.execute(SocketWindowWordCount.class.getClass().getSimpleName());


	}

}
