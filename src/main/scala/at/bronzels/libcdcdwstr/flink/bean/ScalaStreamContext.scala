package at.bronzels.libcdcdwstr.flink.bean

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment


class ScalaStreamContext(var env: StreamExecutionEnvironment, var tableEnv: StreamTableEnvironment) {
}
