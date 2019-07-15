package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Author: LiYahui
 * @Date: Created in  2019/7/14 17:17
 * @Description: TODO flink union java
 * @Version: V1.0
 */
public class UnionDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafka010Data = getMockUpkafka010(env);
		DataStream<MockUpModel> unionDS = kafka010Data.union(kafka010Data,kafka010Data);
		unionDS.countWindowAll(5).max("age")
				.print().setParallelism(1);
		env.execute("kafka010 demo");
	}
}
