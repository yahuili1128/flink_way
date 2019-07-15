package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
  * @Description : flink connect demo
  * @Author : LiYahui
  * @Date : 2019-07-15 18:24
  * @Version : V1.0
  */
object ConnectDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockup: DataStream[ScalaSourceKafka010.MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		val personDS: DataStream[ScalaSourceKafka010.Person] = ScalaSourceKafka010.getPersonFromKafka(env)
		val connectedDS: ConnectedStreams[ScalaSourceKafka010.MockUp, ScalaSourceKafka010.Person] = mockup.connect(personDS)
		// 暂时放缓，不知道如何处理


		env.execute(s"${this.getClass.getSimpleName}")
	}

}
