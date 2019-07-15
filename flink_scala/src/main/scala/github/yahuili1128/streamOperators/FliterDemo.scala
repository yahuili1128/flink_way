package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import github.yahuili1128.connector.ScalaSourceKafka010.MockUp
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
  * @Description : flink filter scala
  * @Author : LiYahui
  * @Date : 2019-07-15 17:53
  * @Version : V1.0
  */
object FliterDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val mockup: DataStream[MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
		//		过滤出年龄大于30的
		//		mockup.filter(_.age > 30)
		//				.print()
		//				.setParallelism(1)
		//		过滤出性别为女性的
		val resultDS: DataStream[MockUp] = mockup.filter(new FilterFunction[MockUp] {
			override def filter(value: MockUp): Boolean = {

				if (value.gender.equals("male")) {
					true
				} else {
					false
				}
			}
		})
		resultDS.print().setParallelism(1)

		env.execute(s"${this.getClass.getSimpleName}")
	}

}
