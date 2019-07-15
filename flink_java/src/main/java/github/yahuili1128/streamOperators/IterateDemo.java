package github.yahuili1128.streamOperators;

import github.yahuili1128.connector.SourceKafka010;
import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description : flink  iterate java
 * @Author : LiYahui
 * @Date : 2019-07-15 13:46
 * @Version : V1.0
 */
public class IterateDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> mockUpkafka010 = SourceKafka010.getMockUpkafka010(env);
		IterativeStream<MockUpModel> iterativeStream = mockUpkafka010.iterate();
		SingleOutputStreamOperator<MockUpModel> mockMapDS = iterativeStream
				.map(new MapFunction<MockUpModel, MockUpModel>() {
					@Override
					public MockUpModel map(MockUpModel value) throws Exception {
						MockUpModel mock = new MockUpModel();
						mock.setGender(value.gender);
						mock.setName(value.name);
						mock.setAge(value.age + 2);
						mock.setTimestamp(value.timestamp);
						return mock;
					}
				});
		SingleOutputStreamOperator<MockUpModel> feedback = mockMapDS.filter(new FilterFunction<MockUpModel>() {
			@Override
			public boolean filter(MockUpModel value) throws Exception {

				return value.age > 50;
			}
		});
		iterativeStream.closeWith(feedback);
		SingleOutputStreamOperator<MockUpModel> resultDS = mockMapDS.filter(new FilterFunction<MockUpModel>() {
			@Override
			public boolean filter(MockUpModel value) throws Exception {
				return value.age < 40;
			}
		});
		resultDS.print().setParallelism(1);

		env.execute("kafka010 demo");
	}
}
