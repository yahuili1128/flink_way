package github.yahuili1128.streamOperators

import org.apache.flink.streaming.api.scala._

/**
  * @Description :  flink flatMap scala
  * @Author : LiYahui
  * @Date : 2019-07-15 17:41
  * @Version : V1.0
  */
object FlatMapDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val list = new scala.collection.mutable.ListBuffer[String]
		list.++("I want to ")
		list.++("have a nice")
		list.++("day")
		val data: DataStream[String] = env.fromCollection(list)
		val resDS: DataStream[(String, Int)] = data.flatMap(_.split(" "))
				.map((_, 1))
				.keyBy(_._1)
				.sum(1)
		resDS.print().setParallelism(1)
		env.execute(s"${this.getClass.getSimpleName}")
	}

}
