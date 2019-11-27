package at.bronzels.libcdcdwstr.flink.source

import java.util.Properties

import at.bronzels.libcdcdwstr.bean.SourceRecordKafkaJsonNode
import at.bronzels.libcdcdwstr.flink.bean.ScalaStreamContext
import at.bronzels.libcdcdwstr.flink.util
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.slf4j.LoggerFactory

class KafkaJsonNodeScalaStream(val prefixWithPrj: String, var strNameCliInput: String, var streamContext: ScalaStreamContext, var strZkQuorum: String, var strBrokers : String, var strListTopic: java.util.List[String]) {
  final private val LOG = LoggerFactory.getLogger(classOf[KafkaJsonNodeScalaStream])

  def getStream: DataStream[SourceRecordKafkaJsonNode] = {
    val cfgGroup = strNameCliInput
    LOG.debug("cfgGroup:{}", cfgGroup)
    val props = new Properties
    props.setProperty("zookeeper.connect", strZkQuorum)
    props.setProperty("bootstrap.servers", strBrokers)
    props.setProperty("group.id", cfgGroup)
    //FlinkKafkaConsumer<SourceRecordKafkaJsonNode> consumer;
    var consumer: FlinkKafkaConsumer011[SourceRecordKafkaJsonNode] = null
    val schema = new KafkaDeserializationJsonNodeSchema
    consumer = //new FlinkKafkaConsumer<>(strListTopic, schema, props);
      new FlinkKafkaConsumer011[SourceRecordKafkaJsonNode](strListTopic, schema, props)
    consumer.setStartFromLatest
    var ret = streamContext.env.addSource(consumer)
    ret
  }
}