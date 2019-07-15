package github.yahuili1128.util;

import com.alibaba.fastjson.JSON;
import github.yahuili1128.pojo.MockUpModel;
import github.yahuili1128.pojo.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


/**
 * @Description :kafka生产者
 * @Author : LiYahui
 * @Date : 2019-06-25 22:20
 * @Version : V1.0
 */
public class KafkaProducerUtil {

	public static void sendMessageToKafkaTopic(MockUpModel pojo) {
		Properties props = new Properties();
		props.put("bootstrap.servers", Parameters.BROKER_LIST());
		//key 序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//value 序列化
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer producer = new KafkaProducer<String, String>(props);
		// 以json的方式发送消息
		ProducerRecord<String, String> record = new ProducerRecord<>(Parameters.TOPIC(), null, null,
				(String) JSON.toJSONString(pojo));
		producer.send(record);
		producer.close();

	}

	public static void sendMessageToKafkaTopic2(MockUpModel pojo) {
		Properties props = new Properties();
		props.put("bootstrap.servers", Parameters.BROKER_LIST());
		//key 序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//value 序列化
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer producer = new KafkaProducer<String, String>(props);
		// 以json的方式发送消息
		ProducerRecord<String, String> record = new ProducerRecord<>(Parameters.TOPIC2(), null, null,
				(String) JSON.toJSONString(pojo));
		producer.send(record);
		producer.close();

	}

	public static void sendPersonkafka(Person pojo) {
		Properties props = new Properties();
		props.put("bootstrap.servers", Parameters.BROKER_LIST());
		//key 序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//value 序列化
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer producer = new KafkaProducer<String, String>(props);
		// 以json的方式发送消息
		ProducerRecord<String, String> record = new ProducerRecord<>(Parameters.TOPIC3(), null, null,
				(String) JSON.toJSONString(pojo));
		producer.send(record);
		producer.close();

	}


	/**
	 * 对测试数据进行赋值
	 *
	 * @throws InterruptedException
	 */
	public static void sendMockUpModel() throws InterruptedException {
		String names[] = {"liyahui", "yahui", "xiaoli"};
		int ages[] = {16, 28, 36};
		Random random = new Random();
		MockUpModel mockUpModel = new MockUpModel();
		int i = 1;
		while (true) {
			//			生成随机数
			int ran = random.nextInt(3);
			mockUpModel.setName(names[ran]);
			if (ran % 2 == 0) {
				mockUpModel.setAge(ages[ran] - ran);
				mockUpModel.setGender("male");
			} else {
				mockUpModel.setAge(ages[ran] + ran);
				mockUpModel.setGender("female");
			}
			mockUpModel.setTimestamp(System.currentTimeMillis());
			sendMessageToKafkaTopic(mockUpModel);
			Thread.sleep(1000);
			System.out.println("topic发送的是第---" + i + "---条数据");
			i++;
		}


	}
	public static void sendMockUpModelTopic2() throws InterruptedException {
		String names[] = {"liyanjie", "yanjie", "xiaojie"};
		int ages[] = {26, 18, 33};
		Random random = new Random();
		MockUpModel mockUpModel = new MockUpModel();
		int i = 1;
		while (true) {
			//			生成随机数
			int ran = random.nextInt(3);
			mockUpModel.setName(names[ran]);
			if (ran % 2 == 0) {
				mockUpModel.setAge(ages[ran] - ran);
				mockUpModel.setGender("male");
			} else {
				mockUpModel.setAge(ages[ran] + ran);
				mockUpModel.setGender("female");
			}
			mockUpModel.setTimestamp(System.currentTimeMillis());
			sendMessageToKafkaTopic2(mockUpModel);
			Thread.sleep(3000);
			System.out.println("topic2发送的是第---" + i + "---条数据");
			i++;
		}


	}

	public static void sendPerson() throws InterruptedException {
		//		String names[] = {"liyahui", "yahui", "xiaoli"};
		String names[] = {"gaddafi", "sophia", "lee"};
		String address[] = {"beijing", "hangzhou", "nanjing"};
		String deps[] = {"bigdata", "alg", "liq"};
		String levels[] = {"manager", "engineer", "director"};
		Random ran = new Random();
		Person person = new Person();
		int i = 1;
		while (true) {
			//			生成随机数
			int index = ran.nextInt(3);
			person.setName(names[index]);
			person.setAddress(address[index]);
			person.setDepartment(deps[index]);
			person.setLevel(levels[index]);
			sendPersonkafka(person);
			//			每隔两秒发送一次
			Thread.sleep(2000);
			System.out.println("发送person类第---" + i + "--数据");
			i++;
		}

	}


	public static void main(String[] args) throws InterruptedException {
		sendMockUpModel();
		sendMockUpModelTopic2();
	}
}
