package at.bronzels.libcdcdwstr.flink

import at.bronzels.libcdcdw.util.MyLog4j2

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

object FrameworkScalaInf {
  var env: StreamExecutionEnvironment = null
  var tableEnv: StreamTableEnvironment = null

  var launchedMS = 0L
  var appName: String = null

  var parallelism = 0L

  def setupEnv(flinkInputParallelism4Local: Integer, isOperatorChainDisabled: Boolean): Unit = {
    val params = ParameterTool.fromSystemProperties
    var env:StreamExecutionEnvironment = null
    if (flinkInputParallelism4Local != null) {
      val configuration = new Configuration
      configuration.setLong("web.timeout", 100000L)
      configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
      configuration.setString("jobmanager.rpc.port", "6133")
      configuration.setString("rest.port: 8089", "rest.port: 8086")
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
      env.setParallelism(flinkInputParallelism4Local)
    }
    else env = StreamExecutionEnvironment.getExecutionEnvironment
    val finalEnv = env
    if (isOperatorChainDisabled) env.disableOperatorChaining
    parallelism = env.getParallelism
    // make parameters available in the web interface
    val data = new java.util.HashMap[String, String]() {}
    data.put(MyLog4j2.MARKER_NAME_COMMONAPP_launchedms, String.valueOf(FrameworkScalaInf.launchedMS))
    data.put(MyLog4j2.MARKER_NAME_COMMONAPP_appname, FrameworkScalaInf.appName)
    val additionalParameterTool = ParameterTool.fromMap(data)
    val mergedParams = params.mergeWith(additionalParameterTool)
    env.getConfig.setGlobalJobParameters(mergedParams)
    FrameworkScalaInf.env = env
    //final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(finalEnv);
    val tableEnv = StreamTableEnvironment.create(env)
    FrameworkScalaInf.tableEnv = tableEnv
  }
}
