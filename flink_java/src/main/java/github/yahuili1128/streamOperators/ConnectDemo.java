package github.yahuili1128.streamOperators;

import github.yahuili1128.connector.SourceKafka010;
import github.yahuili1128.pojo.MockUpModel;
import github.yahuili1128.pojo.Person;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Description :  flink connect java
 * @Author : LiYahui
 * @Date : 2019-07-15 13:02
 * @Version : V1.0
 */
public class ConnectDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<MockUpModel> mockUpkafka010 = SourceKafka010.getMockUpkafka010(env);
		SingleOutputStreamOperator<Person> personKafka010 = SourceKafka010.getPersonKafka010(env);
		ConnectedStreams<MockUpModel, Person> resDS = mockUpkafka010.connect(personKafka010);
		// 接下来的操作待定，还不知道怎么去处理



		env.execute("kafka010 demo");
	}
}
