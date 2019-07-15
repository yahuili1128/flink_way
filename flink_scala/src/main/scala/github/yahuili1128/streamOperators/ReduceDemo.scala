package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import github.yahuili1128.connector.ScalaSourceKafka010.MockUp
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
  * @Description : flink reduce scala
  * @Author : LiYahui
  * @Date : 2019-07-15 18:00
  * @Version : V1.0
  */
object ReduceDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockup: DataStream[ScalaSourceKafka010.MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		val resDS: DataStream[ScalaSourceKafka010.MockUp] = mockup
				.keyBy(_.gender)
				.reduce(new ReduceFunction[ScalaSourceKafka010.MockUp] {
					override def reduce(t: ScalaSourceKafka010.MockUp, t1: ScalaSourceKafka010.MockUp): ScalaSourceKafka010.MockUp = {
						MockUp(t.name + "--" + t1.name, t.gender, t.timestamp, t.age + t1.age)
					}
				})
		resDS.print().setParallelism(1)

		env.execute(s"${this.getClass.getSimpleName}")
	}
}
