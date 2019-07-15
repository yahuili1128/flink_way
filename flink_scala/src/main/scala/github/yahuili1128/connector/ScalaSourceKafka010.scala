package github.yahuili1128.connector

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import github.yahuili1128.util.Parameters
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * @Description :  flink 消费kafka中的数据
  * @Author : LiYahui
  * @Date : 2019-06-28 15:57
  * @Version : V1.0
  */
object ScalaSourceKafka010 {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockupDS: DataStream[MockUp] = getMockUpFromKafka(env)
		mockupDS
				.print().setParallelism(1)

		env.execute(s"${this.getClass.getSimpleName}")

	}

	/**
	  * 从kafka中消费多个topic
	  *
	  * @param env
	  * @return
	  */
	def getMockUpFromKafka(env: StreamExecutionEnvironment): DataStream[MockUp] = {
		val props = new Properties()
		props.put("bootstrap.servers", Parameters.BROKER_LIST)
		props.put("group.id", Parameters.GROUP_ID)
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("auto.offset.reset", "latest")
		//		从kafka中消费多个topic
		import scala.collection.JavaConversions._
		val topics = new scala.collection.mutable.ListBuffer[String]
		topics.add(Parameters.TOPIC)
		topics.add(Parameters.TOPIC2)
		val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String](topics, new SimpleStringSchema(), props))
		val mockupDS: DataStream[MockUp] = kafkaDS
				.rebalance
				.map(line => {
					val jsonObj: JSONObject = JSON.parseObject(line)
					val name: String = jsonObj.getString("name")
					val gender: String = jsonObj.getString("gender")
					val timestamp: Long = jsonObj.getLong("timestamp").toLong
					val age: Int = jsonObj.getInteger("age").toInt
					MockUp(name, gender, timestamp, age)
				})
		mockupDS
	}

	/**
	  * 消费kafka中的person topic
	  *
	  * @param env
	  * @return
	  */
	def getPersonFromKafka(env: StreamExecutionEnvironment): DataStream[Person] = {
		val props = new Properties()
		props.put("bootstrap.servers", Parameters.BROKER_LIST)
		props.put("group.id", Parameters.GROUP_ID)
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("auto.offset.reset", "latest")
		val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String](Parameters.TOPIC3, new SimpleStringSchema(), props))
		val personDS: DataStream[Person] = kafkaDS
				.rebalance
				.map(line => {
					val jsonObj: JSONObject = JSON.parseObject(line)
					val name: String = jsonObj.getString("name")
					val address: String = jsonObj.getString("address")
					val department: String = jsonObj.getString("department")
					val level: String = jsonObj.getString("level")
					Person(name, address, department, level)
				})
		personDS
	}


	case class Person(name: String, address: String, department: String, level: String)

	// 解析kafka中的json
	case class MockUp(name: String, gender: String, timestamp: Long, age: Int)

}
