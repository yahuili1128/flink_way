package github.yahuili1128.streamOperators;

import github.yahuili1128.connector.SourceKafka010;
import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Description :  flink select java
 * @Author : LiYahui
 * @Date : 2019-07-15 13:36
 * @Version : V1.0
 */
public class SelectDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> mockUpkafka010 = SourceKafka010.getMockUpkafka010(env);
		SplitStream<MockUpModel> splitDS = mockUpkafka010.split(new OutputSelector<MockUpModel>() {
			@Override
			public Iterable<String> select(MockUpModel value) {
				ArrayList<String> output = new ArrayList<>();
				if (value.gender.equals("male")) {
					output.add("male");
				} else {
					output.add("famle");
				}

				return output;
			}
		});
		//		splitDS.print().setParallelism(1);
		//		MockUpModel(name=yahui, gender=female, timestamp=1563168888165, age=29)
		// select 算子已经过期
		DataStream<MockUpModel> maleDS = splitDS.select("male");
		DataStream<MockUpModel> famleDS = splitDS.select("famle");
		maleDS.print().setParallelism(1);
		famleDS.print().setParallelism(1);

		env.execute("kafka010 demo");
	}
}
