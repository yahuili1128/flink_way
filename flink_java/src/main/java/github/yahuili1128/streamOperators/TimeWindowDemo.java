package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Description : flink window java
 * @Author : LiYahui
 * @Date : 2019-06-26 16:57
 * @Version : V1.0
 */
public class TimeWindowDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafkaData = getMockUpkafka010(env);

		kafkaData.keyBy(line -> line.gender).timeWindow(Time.seconds(20)).sum("age")
				.map(new MapFunction<MockUpModel, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(MockUpModel value) throws Exception {
						String gender = value.gender;
						int age = value.age;
						return new Tuple2<>(gender, age);
					}
				}).print().setParallelism(1);
		env.execute("flink kafka010 demo");
	}
}
