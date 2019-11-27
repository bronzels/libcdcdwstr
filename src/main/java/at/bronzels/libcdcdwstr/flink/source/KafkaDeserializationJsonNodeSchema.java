package at.bronzels.libcdcdwstr.flink.source;

import at.bronzels.libcdcdwstr.bean.SourceRecordKafkaJsonNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaDeserializationJsonNodeSchema extends AbstractKafkaDeserializationSchema<SourceRecordKafkaJsonNode> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public SourceRecordKafkaJsonNode deserialize(byte[] keyBytes, byte[] valueBytes, String topic, int partition, long offset) throws IOException {
        long msgts = System.currentTimeMillis();
        String guid = String.valueOf(partition) + at.bronzels.libcdcdw.Constants.commonSep + offset;
        LOG.debug("guid:{}, msgts:{}, topic:{}, partition:{}, offset:{}", guid, msgts, topic, partition, offset);
        String key = keyBytes != null ? new String(keyBytes, "UTF-8") : null;
        JsonNode value = valueBytes != null ? mapper.readTree(valueBytes) : null;
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
