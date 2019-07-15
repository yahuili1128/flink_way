package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Description :flink java reduce
 * @Author : LiYahui
 * @Date : 2019-06-26 11:27
 * @Version : V1.0
 */
public class ReduceDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafka010Data = getMockUpkafka010(env);
		//		lambda +java
		kafka010Data.keyBy(line -> line.gender).reduce(new ReduceFunction<MockUpModel>() {
			@Override
			public MockUpModel reduce(MockUpModel value1, MockUpModel value2) throws Exception {
				MockUpModel mockUpModel = new MockUpModel();
				mockUpModel.name = value1.name + "--" + value2.name;
				mockUpModel.gender = value1.gender;
				mockUpModel.age = (value1.age + value2.age) / 2;
				return mockUpModel;

			}
		}).print().setParallelism(1);

		env.execute("flink kafka010 demo");
	}
}
