package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import github.yahuili1128.connector.ScalaSourceKafka010.MockUp
import org.apache.flink.streaming.api.scala._

/**
  * @Description : flink iterate scala
  * @Author : LiYahui
  * @Date : 2019-07-15 18:40
  * @Version : V1.0
  */
object IterateDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockup: DataStream[ScalaSourceKafka010.MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		val iterateMockupDS: DataStream[MockUp] = mockup.iterate {
			iteration => {
				val addAgeDS: DataStream[MockUp] = iteration.map(line => {
					MockUp(line.name, line.gender, line.timestamp, line.age + 2)
				})
				//		feedback , 过滤条件
				(addAgeDS.filter(_.age > 50), addAgeDS.filter(_.age < 40))
			}
		}

		iterateMockupDS.print().setParallelism(1)

		env.execute(s"${this.getClass.getSimpleName}")
	}
}
