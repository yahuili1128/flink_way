package github.yahuili1128.streamOperators;

import github.yahuili1128.pojo.MockUpModel;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static github.yahuili1128.connector.SourceKafka010.getMockUpkafka010;

/**
 * @Description : flink fold java
 * @Author : LiYahui
 * @Date : 2019-06-26 11:39
 * @Version : V1.0
 */
public class FoldDemo {
	public static void main(String[] args) throws Exception {
		//		fold
		//		Fold 通过将最后一个数据流与当前记录组合来推出 KeyedStream。 它会发回数据流

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> kafka010Data = getMockUpkafka010(env);
		kafka010Data.keyBy(line -> line.age).fold("flag", new FoldFunction<MockUpModel, String>() {
			@Override
			public String fold(String accumulator, MockUpModel value) throws Exception {
				return accumulator + "===" + value;
			}
		}).print().setParallelism(1);

		//		flag===MockUpModel(name=liyahui-0, gender=male, timestamp=1561520590346, age=2)===MockUpModel(name=liyahui-2, gender=male, timestamp=1561520600943, age=2)===MockUpModel(name=liyahui-4, gender=male, timestamp=1561520610985, age=2)===MockUpModel(name=liyahui-6, gender=male, timestamp=1561520621015, age=2)

		env.execute("flink kafka010 demo");

	}
}
