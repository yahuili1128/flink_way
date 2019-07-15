package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Description :flink map java   ,DataStream Operators :https://ci.apache.org/projects/flink/flink-docs-master/dev/stream/operators/index.html
 * @Author : LiYahui
 * @Date : 2019-06-26 10:16
 * @Version : V1.0
 */
public class MapDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafkaData = getMockUpkafka010(env);
		SingleOutputStreamOperator<Integer> addAgeDS = getAddAgeDS(kafkaData);
		addAgeDS
				.print().setParallelism(1);

		//SingleOutputStreamOperator<MockUpModel> addAgePojo = getAddAgePojo(kafkaData);
		//addAgePojo
		//		.print().setParallelism(1);


		env.execute("flink kafka010 demo");

	}

	/**
	 * 获取的是整个pojo
	 *  MockUpModel(name=yahui, gender=female, timestamp=1563090399305, age=34)
	 * @param kafkaData
	 * @return
	 */
	private static SingleOutputStreamOperator<MockUpModel> getAddAgePojo(SingleOutputStreamOperator<MockUpModel> kafkaData) {
		//		返回的是MockUpModel pojo类，其中age字段均+5
		return kafkaData.map(new MapFunction<MockUpModel, MockUpModel>() {
			@Override
			public MockUpModel map(MockUpModel value) throws Exception {
				MockUpModel mockUpModel = new MockUpModel();
				mockUpModel.name = value.name;
				mockUpModel.gender = value.gender;
				mockUpModel.age = value.age + 5;
				mockUpModel.timestamp = value.timestamp;
				return mockUpModel;

			}
		});
	}

	/**
	 * 获取的只是每个元素加过5的age数字
	 * 34
	 *
	 * @param kafkaData
	 * @return
	 */
	private static SingleOutputStreamOperator<Integer> getAddAgeDS(SingleOutputStreamOperator<MockUpModel> kafkaData) {
		//		获取的只是message中age字段+5后的数据
		return kafkaData.map(line -> line.age + 5);
	}
}
