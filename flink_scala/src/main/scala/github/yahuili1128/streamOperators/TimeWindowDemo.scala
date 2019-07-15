package github.yahuili1128.streamOperators

import github.yahuili1128.connector.ScalaSourceKafka010
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Author:  LiYahui
  * Date:  Created in  2019/7/15 21:15
  * Description: TODO  flink window scala
  * Version: V1.0         
  */
object TimeWindowDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val mockup: DataStream[ScalaSourceKafka010.MockUp] = ScalaSourceKafka010.getMockUpFromKafka(env)
    val resDS = mockup.keyBy(_.gender)
        .timeWindow(Time.seconds(5))
        .sum("age")
    resDS.print().setParallelism(1)

    env.execute(s"${this.getClass.getSimpleName}")

  }
}
