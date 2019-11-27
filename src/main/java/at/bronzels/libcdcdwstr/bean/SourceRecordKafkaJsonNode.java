package at.bronzels.libcdcdwstr.bean;

import at.bronzels.libcdcdwstr.flink.util.MyJackson;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class SourceRecordKafkaJsonNode extends AbstractSourceRecordKafka<JsonNode> implements RecordJsonNodeInf {

    public SourceRecordKafkaJsonNode(String key, JsonNode value, String topic, int partition, long offset, long msgts, String guid) {
        super(key, value, topic, partition, offset, msgts, guid);
    }

    @Override
    public String toString() {
        ObjectNode node =  JsonNodeFactory.instance.objectNode();
        node.put("key", getKey());
        node.put("value", getValue());
        node.put("topic", getTopic());
        node.put("partition", getPartition());
        node.put("offset", getOffset());
        node.put("msgts", getMsgts());
        node.put("guid", getGuid());
        return MyJackson.getString(node);
    }

}
