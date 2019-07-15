package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Description : flink flatmap java
 * @Author : LiYahui
 * @Date : 2019-06-26 10:25
 * @Version : V1.0
 */
public class FlatMapDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafkaData = getMockUpkafka010(env);

		//		这个操作不能反应flatMap的算子作用,下面的作用相当于filter,输出结果为MockUpModel(name=liyahui-0, gender=male, timestamp=1561516105296, age=0)
		kafkaData.flatMap(new FlatMapFunction<MockUpModel, MockUpModel>() {
			@Override
			public void flatMap(MockUpModel value, Collector<MockUpModel> out) throws Exception {
				if (value.age % 2 == 0) {
					out.collect(value);
				}
			}
		}).print().setParallelism(1);
		//	 flatmap，是将嵌套集合转换并平铺成非嵌套集合。最好的解释详见官网的释义
		/*
		dataStream.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out)
					throws Exception {
				for(String word: value.split(" ")){
					out.collect(word);
				}
			}
		});
		*/


		env.execute("flink kafka010 demo");
	}
}
