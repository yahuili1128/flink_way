package github.yahuili1128.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

import static github.yahuili1128.util.Parameters.*;

/**
 * @Author: LiYahui
 * @Date: Created in  2019/7/14 10:14
 * @Description: TODO  kafka 消费者demo
 * @Version: V1.0
 */
public class KafkaConsumerUtil {
	public static void main(String[] args) {
		Properties props = new Properties();
		//集群地址，多个地址用"，"分隔
		props.put("bootstrap.servers", BROKER_LIST);
		//设置消费者的group id
		props.put("group.id", GROUP_ID);
		//如果为真，consumer所消费消息的offset将会自动的同步到zookeeper。如果消费者死掉时，由新的consumer使用继续接替
		props.put("enable.auto.commit", "true");
		//consumer向zookeeper提交offset的频率
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		//反序列化
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//创建消费者
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		// 订阅topic，可以为多个用,隔开，此处订阅了"test-partition-1", "test"这两个主题
		consumer.subscribe(Arrays.asList(TOPIC1));
		//持续监听
		while (true) {
			//poll频率
			ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				System.out.println("在topic中拉取的数据为" + consumerRecord.value());
			}
		}

	}
}
