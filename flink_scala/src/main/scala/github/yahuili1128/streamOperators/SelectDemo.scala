package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import github.yahuili1128.connector.ScalaSourceKafka010.MockUp
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
  * @Description : flink select scala
  * @Author : LiYahui
  * @Date : 2019-07-15 18:35
  * @Version : V1.0
  */
object SelectDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockup: DataStream[ScalaSourceKafka010.MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		val splitDS: SplitStream[MockUp] = mockup.split((line: MockUp) => line.gender match {
			case "male" => List("male")
			case "female" => List("female")
		})
		splitDS.select("male")
				.print()
				.setParallelism(1)

		env.execute(s"${this.getClass.getSimpleName}")
	}

}
