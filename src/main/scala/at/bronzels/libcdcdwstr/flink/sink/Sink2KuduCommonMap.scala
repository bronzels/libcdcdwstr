package at.bronzels.libcdcdwstr.flink.sink

import at.bronzels.libcdcdw.bean.MyLogContext
import at.bronzels.libcdcdw.conf.{KuduTableEnvConf, DistLockConf}
import at.bronzels.libcdcdw.kudu.pool.MyKudu
import at.bronzels.libcdcdwstr.flink.global.MyParameterTool
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration

abstract class Sink2KuduCommonMap[IN, OUT](val kuduTableEnvConf: KuduTableEnvConf, val distLockConf: DistLockConf, val isSrcFieldNameWTUpperCase: Boolean) extends RichFlatMapFunction[IN, OUT] {
  private lazy val _dwsynctsKuduFieldIndexHolder = new ThreadLocal[Integer]
  private lazy val myKuduHolder = new ThreadLocal[MyKudu]

  var logContext:MyLogContext = null

  def getKuduAndTsIndex(): (MyKudu, Integer) = {
    var myKudu = myKuduHolder.get
    if (kuduTableEnvConf != null) {
      if (myKudu == null) {
        myKudu = getMyKuduOpenedAndThreadlocalSet
        myKuduHolder.set(myKudu)
      }
    }
    val _dwsynctsKuduFieldIndex = _dwsynctsKuduFieldIndexHolder.get
    (myKudu, _dwsynctsKuduFieldIndex)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val context = getRuntimeContext
    logContext = MyParameterTool.getLogContext(context)
  }

  override def close(): Unit = {
  }

  private def getMyKuduOpenedAndThreadlocalSet: MyKudu = {
    var groupbyRowIndex2KuduFieldIndexMap: java.util.Map[Integer, Integer] = null
    var seriestsKuduFieldIndex: Integer = null
    groupbyRowIndex2KuduFieldIndexMap = new java.util.HashMap[Integer, Integer]
    val myKudu: MyKudu = new MyKudu(isSrcFieldNameWTUpperCase, kuduTableEnvConf.getCatalog, kuduTableEnvConf.getDbUrl, kuduTableEnvConf.getDbDatabase, kuduTableEnvConf.getTblName, distLockConf)
    myKudu.open()
    val kuduFieldName2IndexMap: java.util.Map[String, Integer] = myKudu.getName2IndexMap
    seriestsKuduFieldIndex = kuduFieldName2IndexMap.get(at.bronzels.libcdcdw.Constants.FIELDNAME_MODIFIED_TS)
    _dwsynctsKuduFieldIndexHolder.set(seriestsKuduFieldIndex)
    myKudu
  }

}
