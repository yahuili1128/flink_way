package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import org.apache.flink.streaming.api.scala._

/**
  * @Description :flink aggregrations scala
  * @Author : LiYahui
  * @Date : 2019-07-15 18:18
  * @Version : V1.0
  */
object AggregationsDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockup: DataStream[ScalaSourceKafka010.MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		val maxAgeDS: DataStream[ScalaSourceKafka010.MockUp] = mockup.keyBy(_.gender)
				.maxBy("age")
		maxAgeDS.print().setParallelism(1)
		val ageDS: DataStream[ScalaSourceKafka010.MockUp] = mockup.keyBy(_.gender).max("age")
		ageDS.print().setParallelism(1)

		env.execute(s"${this.getClass.getSimpleName}")
	}

}
