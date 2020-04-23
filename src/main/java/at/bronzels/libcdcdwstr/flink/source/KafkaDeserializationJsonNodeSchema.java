package at.bronzels.libcdcdwstr.flink.source;

import at.bronzels.libcdcdwstr.bean.SourceRecordKafkaJsonNode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class KafkaDeserializationJsonNodeSchema extends AbstractKafkaDeserializationSchema<SourceRecordKafkaJsonNode> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private ObjectMapper mapper = new ObjectMapper();
    //private ObjectReader reader = new ObjectMapper().reader();
    //private SimpleStringSchema strSchema = new SimpleStringSchema();
    //private JSONKeyValueDeserializationSchema nodeSchema = new JSONKeyValueDeserializationSchema(true);

    @Override
    public SourceRecordKafkaJsonNode deserialize(byte[] keyBytes, byte[] valueBytes, String topic, int partition, long offset) throws IOException {
        long msgts = System.currentTimeMillis();
        String guid = String.valueOf(partition) + at.bronzels.libcdcdw.Constants.commonSep + offset;
        LOG.debug("guid:{}, msgts:{}, topic:{}, partition:{}, offset:{}", guid, msgts, topic, partition, offset);
        String key = "";//keyBytes != null ? new String(keyBytes, "UTF-8") : null;
        long msgts_startStr = System.currentTimeMillis();
        //String strValue = new String(valueBytes, "UTF-8");
        //String strValue = new String(valueBytes, StandardCharsets.US_ASCII);
        //String strValue = valueBytes != null ? new String(valueBytes) : null;
        //String strValue = valueBytes != null ? new String(valueBytes, StandardCharsets.UTF_16) : null;
        //String strValue = valueBytes != null ? strSchema.deserialize(valueBytes) : null;
        long msgts_takenStr = System.currentTimeMillis() - msgts_startStr;
        LOG.debug("guid:{}, msgts_takenStr:{}", guid, msgts_takenStr);
        long msgts_start = System.currentTimeMillis();
        JsonNode value = valueBytes != null ? mapper.readTree(valueBytes) : null;
        //JsonNode value = valueBytes != null ? mapper.readTree(strValue) : null;
        //JsonNode value = (offset == 192 || offset == 215 | offset == 729) ? null : (valueBytes != null ? reader.readTree(new ByteArrayInputStream(valueBytes)) : null);
        /*
        JsonNode value = null;
        try{
            value = valueBytes != null ? nodeSchema.deserialize(new ConsumerRecord<byte[], byte[]>(topic, partition, offset, keyBytes, valueBytes)).get("value") : null;
        } catch (Exception e) {
            e.printStackTrace();
        }
         */
        long msgts_taken = System.currentTimeMillis() - msgts_start;
        LOG.debug("guid:{}, msgts_taken:{}", guid, msgts_taken);
        LOG.debug("guid:{}, ctmilli:{}", guid, System.currentTimeMillis());

        SourceRecordKafkaJsonNode record = new SourceRecordKafkaJsonNode(key, value, topic, partition, offset, msgts, guid);

        LOG.debug("guid:{}, ctmilli:{}", guid, System.currentTimeMillis());
        return record;
    }

    @Override
    public boolean isEndOfStream(SourceRecordKafkaJsonNode record) {
        return false;
    }

    @Override
    public TypeInformation<SourceRecordKafkaJsonNode> getProducedType() {
        return TypeInformation.of(SourceRecordKafkaJsonNode.class);
    }

}
