package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Description : flink union scala
  * @Author : LiYahui
  * @Date : 2019-07-15 18:21
  * @Version : V1.0
  */
object UnionDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockup: DataStream[ScalaSourceKafka010.MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		val unionDS: DataStream[ScalaSourceKafka010.MockUp] = mockup.union(mockup, mockup)
		unionDS.print().setParallelism(1)


		env.execute(s"${this.getClass.getSimpleName}")
	}

}
