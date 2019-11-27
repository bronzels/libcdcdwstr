package at.bronzels.libcdcdwstr.flink.sink

import at.bronzels.libcdcdw.conf.{KuduTableEnvConf, DistLockConf}
import at.bronzels.libcdcdw.kudu.myenum.KuduWriteEnum
import at.bronzels.libcdcdw.kudu.myenum.KuduWriteEnum._
import at.bronzels.libcdcdwstr.flink.util.{MyJackson, MyKuduTypeValue}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.util.Collector

class JsonNode2KuduMap(override val kuduTableEnvConf: KuduTableEnvConf, override val distLockConf: DistLockConf, override val isSrcFieldNameWTUpperCase: Boolean) extends Sink2KuduCommonMap[(KuduWriteEnum, JsonNode), String](kuduTableEnvConf, distLockConf, isSrcFieldNameWTUpperCase) {

  override def flatMap(in: (KuduWriteEnum, JsonNode), out: Collector[String]) = {
    val tuple = getKuduAndTsIndex
    val myKudu = tuple._1
    val _dwsynctsKuduFieldIndex = tuple._2

    var ret:String = ""

    val writeEnum = in._1
    val node = in._2
    val mapTuple = MyKuduTypeValue.getCol2AddAndValueMapTuple(myKudu.getName2TypeMap, in._2, isSrcFieldNameWTUpperCase, logContext)
    val newCol2TypeMap = mapTuple.f0
    if(!writeEnum.equals(Delete) && newCol2TypeMap.size() > 0) {
      if(!newCol2TypeMap.isEmpty) {
        myKudu.addColumns(newCol2TypeMap)
      }
    }
    val map = mapTuple.f1
    in._1 match {
      case KuduWriteEnum.Put =>
        ret = myKudu.putStrAsKey(map)
      case KuduWriteEnum.Delete =>
        ret = myKudu.deleteStrAsKey(map)
      case KuduWriteEnum.IncrEmulated =>
        ret = myKudu.incr(map)
      case KuduWriteEnum.SetOnInsert4PropAvailable =>
        ret = myKudu.setOnInsert4PropAvailable(map)
      case _ =>
        ret = "unsupported:" + in._1
    }
    out.collect(ret)
  }
}
