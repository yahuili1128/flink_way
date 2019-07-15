package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Description :flink fliter java
 * @Author : LiYahui
 * @Date : 2019-06-26 10:29
 * @Version : V1.0
 */
public class FliterDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafkaData = getMockUpkafka010(env);
		//  lambda表达式的形式
		//   kafkaData.filter(line -> line.gender.equals("male")).print().setParallelism(1);
		//		输出结果 MockUpModel(name=liyahui-2, gender=male, timestamp=1561518051118, age=2)

		//SingleOutputStreamOperator<MockUpModel> filterDS = getFilterDS(kafkaData);
		//filterDS
		//		.print().setParallelism(1);

		SingleOutputStreamOperator<MockUpModel> filterDS2 = getFilterDS2(kafkaData);
		filterDS2
				.print().setParallelism(1);

		env.execute("flink kafka010 demo");
	}

	/**
	 * 过滤出年龄是偶数的人
	 *
	 * @param kafkaData
	 * @return
	 */
	private static SingleOutputStreamOperator<MockUpModel> getFilterDS2(SingleOutputStreamOperator<MockUpModel> kafkaData) {
		return kafkaData.filter(new FilterFunction<MockUpModel>() {
			@Override
			public boolean filter(MockUpModel value) throws Exception {
				if (value.age % 2 == 0) {
					return true;
				}
				return false;
			}
		});
	}

	/**
	 * lambda 的方式
	 *
	 * @param kafkaData
	 * @return
	 */
	private static SingleOutputStreamOperator<MockUpModel> getFilterDS(SingleOutputStreamOperator<MockUpModel> kafkaData) {
		return kafkaData.filter(line -> line.age % 2 == 0);
	}
}
