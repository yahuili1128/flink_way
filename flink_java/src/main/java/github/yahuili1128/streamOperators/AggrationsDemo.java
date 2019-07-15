package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Description :flink aggrations java
 * @Author : LiYahui
 * @Date : 2019-06-26 11:44
 * @Version : V1.0
 */
public class AggrationsDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafka010Data = getMockUpkafka010(env);
		KeyedStream<MockUpModel, String> keyedStream = kafka010Data.keyBy(line -> line.gender);
		SingleOutputStreamOperator<MockUpModel> sum = keyedStream.sum("age");
		sum.print().setParallelism(1);

		SingleOutputStreamOperator<MockUpModel> max = keyedStream.max("age");
		max.print().setParallelism(1);
		SingleOutputStreamOperator<MockUpModel> maxBy = keyedStream.maxBy("age");
		maxBy.print().setParallelism(1);
		//		max 和 maxBy 之间的区别在于 max 返回流中的最大值，但 maxBy 返回具有最大值的键， min 和 minBy 同理。


		env.execute("flink kafka010 demo");
	}
}
