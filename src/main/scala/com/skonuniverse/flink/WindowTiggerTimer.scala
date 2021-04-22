package com.skonuniverse.flink

import com.skonuniverse.flink.datatype.Message
import com.skonuniverse.flink.source.MessageSource
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._

import java.time.Duration


object WindowTiggerTimer extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env
        .fromSource(
          new MessageSource(Seq(1, 2, 3)),
          WatermarkStrategy
              .forBoundedOutOfOrderness(Duration.ofMillis(50L))
              .withTimestampAssigner(new SerializableTimestampAssigner[Message] {
                override def extractTimestamp(element: Message, recordTimestamp: Long): Long = element.eventTime.getTime
              }),
          "MessageSource"
        )
        .setParallelism(3)
        .print

    env.execute("Window Tigger Timers")
  }
}
