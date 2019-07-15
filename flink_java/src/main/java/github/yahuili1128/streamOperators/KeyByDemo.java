package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Description : flink keyBy java
 * @Author : LiYahui
 * @Date : 2019-06-26 11:21
 * @Version : V1.0
 */
public class KeyByDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafkaData = getMockUpkafka010(env);
		/*KeyedStream<MockUpModel, Integer> keyedStream = getKeyedDS(kafkaData);

		keyedStream
				.print().setParallelism(1);*/

		KeyedStream<MockUpModel, ArrayList<String>> keyedStream2 = getKeyedDS2(kafkaData);

		keyedStream2.print().setParallelism(1);


		env.execute("flink kafka010 demo");
	}

	/**
	 * 以gender/name为分组条件,还可以探索一下用其他的方式
	 *
	 * @param kafkaData
	 * @return
	 */
	private static KeyedStream<MockUpModel, ArrayList<String>> getKeyedDS2(SingleOutputStreamOperator<MockUpModel> kafkaData) {
		return kafkaData.keyBy(new KeySelector<MockUpModel, ArrayList<String>>() {
			@Override
			public ArrayList<String> getKey(MockUpModel mockUpModel) throws Exception {
				ArrayList<String> list = new ArrayList<>();
				list.add(mockUpModel.gender);
				list.add(mockUpModel.name);
				return list;
			}
		});
	}

	/**
	 * 以年龄为分组条件进行keyBy
	 *
	 * @param kafkaData
	 * @return
	 */
	private static KeyedStream<MockUpModel, Integer> getKeyedDS(SingleOutputStreamOperator<MockUpModel> kafkaData) {
		//	lambda 表达式
		//kafkaData.keyBy(line -> line.age).print().setParallelism(1);
		return kafkaData.keyBy(new KeySelector<MockUpModel, Integer>() {
			@Override
			public Integer getKey(MockUpModel value) throws Exception {
				return value.age;
			}
		});
	}
}
