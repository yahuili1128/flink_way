package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import github.yahuili1128.connector.ScalaSourceKafka010.MockUp
import github.yahuili1128.util.Parameters
import org.apache.flink.streaming.api.scala._

/**
  * @Description : flink map scala
  * @Author : LiYahui
  * @Date : 2019-07-15 17:35
  * @Version : V1.0
  */
object MapDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockupDS: DataStream[MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		val addDS: DataStream[MockUp] = mockupDS.map(line => {
			MockUp(line.name + "----add age", line.gender, line.timestamp, line.age + 5)
		})
		addDS.print()
				.setParallelism(1)

		env.execute(s"${this.getClass.getSimpleName}")
	}

}
