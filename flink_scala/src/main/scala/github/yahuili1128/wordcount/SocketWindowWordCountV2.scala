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
object SocketWindowWordCountV2 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val socketData: DataStream[String] = env.socketTextStream("liyahui-01", 9999, '\n')
    val wordcountDS: DataStream[WordCount] = socketData.flatMap(_.split("\\s"))
        .map(WordCount(_, 1L))
        .keyBy(_.word)
        .timeWindow(Time.seconds(5), Time.seconds(1))
        .sum("num")

    wordcountDS.print()
        .setParallelism(1)
    env.execute(s"${this.getClass.getSimpleName}")
  }

  case class WordCount(word: String, num: Long)

}
