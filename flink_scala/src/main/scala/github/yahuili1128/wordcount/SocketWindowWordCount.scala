package github.yahuili1128.wordcount

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Author:  LiYahui
  * Date:  Created in  2019/7/13 16:23
  * Description: TODO  flink 读取socket
  * Version: V1.0         
  */
object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val socketData: DataStream[String] = env.socketTextStream("liyahui-01", 9999, '\n')
    val wordcountDS: DataStream[(String, Int)] = socketData.flatMap(_.split("\\s"))
        .map((_, 1))
        .keyBy(_._1)
        .timeWindow(Time.seconds(5), Time.seconds(1))
        .reduce(new ReduceFunction[(String, Int)] {
          override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = {
            (t._1, t._2 + t1._2)
          }
        })

    //    reduce 的作用可以用sum替代

    wordcountDS.print()
        .setParallelism(1)
    env.execute(s"${this.getClass.getSimpleName}")
  }
}
