package github.yahuili1128.connector;

import com.alibaba.fastjson.JSON;
import github.yahuili1128.pojo.MockUpModel;
import github.yahuili1128.pojo.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

import static github.yahuili1128.util.Parameters.*;

/**
 * @Description : flink从kafka中消费topic数据
 * @Author : LiYahui
 * @Date : 2019-06-25 22:45
 * @Version : V1.0
 */
public class SourceKafka010 {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SingleOutputStreamOperator<MockUpModel> kafkaData = getMockUpkafka010(env);
		kafkaData.print().setParallelism(1);


		env.execute("kafka010 demo");
	}

	/**
	 * 封装flink 读取kafka010中的数据，便于调用
	 *
	 * @param env StreamExecutionEnvironment
	 * @return MockUpModel
	 */
	public static SingleOutputStreamOperator<MockUpModel> getMockUpkafka010(StreamExecutionEnvironment env) {
		Properties props = new Properties();
		props.put("bootstrap.servers", BROKER_LIST);
		props.put("group.id", GROUP_ID);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");
		SingleOutputStreamOperator<MockUpModel> kafkaData = env
				.addSource(new FlinkKafkaConsumer010<String>(TOPIC1, new SimpleStringSchema(), props))
				.map(new MapFunction<String, MockUpModel>() {
					@Override
					public MockUpModel map(String value) throws Exception {
						return JSON.parseObject(value, MockUpModel.class);
					}
				});
		return kafkaData;
	}

	/**
	 * 采用java8的lambda表达式
	 *
	 * @param env
	 * @return
	 */
	public static SingleOutputStreamOperator<MockUpModel> getMockUpKafka010V2(StreamExecutionEnvironment env) {
		Properties props = new Properties();
		props.put("bootstrap.servers", BROKER_LIST);
		props.put("group.id", GROUP_ID);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");
		SingleOutputStreamOperator<MockUpModel> kafkaData = env
				.addSource(new FlinkKafkaConsumer010<String>(TOPIC1, new SimpleStringSchema(), props))
				.map(line -> JSON.parseObject(line, MockUpModel.class));   // lambda 表达式
		return kafkaData;
	}

	/**
	 * 从kafka中消费person  topic数据
	 * @param env
	 * @return
	 */
	public static SingleOutputStreamOperator<Person> getPersonKafka010(StreamExecutionEnvironment env) {
		Properties props = new Properties();
		props.put("bootstrap.servers", BROKER_LIST);
		props.put("group.id", GROUP_ID);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");
		SingleOutputStreamOperator<Person> personData = env
				.addSource(new FlinkKafkaConsumer010<String>(TOPIC2, new SimpleStringSchema(), props))
				.map(line -> JSON.parseObject(line, Person.class));
		return personData;
	}
}
