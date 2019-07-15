package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import org.apache.flink.streaming.api.scala._

/**
  * @Description : flink fold scala
  * @Author : LiYahui
  * @Date : 2019-07-15 18:13
  * @Version : V1.0
  */
object FoldDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockup: DataStream[ScalaSourceKafka010.MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		val resDS: DataStream[String] = mockup.keyBy(_.gender)
				.fold("flag")((line, str) => {
					line + "=======" + str
				})
		resDS.print().setParallelism(1)

		env.execute(s"${this.getClass.getSimpleName}")
	}

}
