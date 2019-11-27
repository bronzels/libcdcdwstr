package at.bronzels.libcdcdwstr.flink.source;

import at.bronzels.libcdcdwstr.bean.SourceRecordKafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaDeserializationSchema extends AbstractKafkaDeserializationSchema<SourceRecordKafka> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    @Override
    public SourceRecordKafka deserialize(byte[] keyBytes, byte[] valueBytes, String topic, int partition, long offset) throws IOException {
        long msgts = System.currentTimeMillis();
        String guid = String.valueOf(partition) + at.bronzels.libcdcdw.Constants.commonSep + offset;
        LOG.debug("guid:{}, msgts:{}, topic:{}, partition:{}, offset:{}", guid, msgts, topic, partition, offset);
        String key = keyBytes != null ? new String(keyBytes, "UTF-8") : null;
        String value = valueBytes != null ? new String(valueBytes, "UTF-8") : null;
        LOG.debug("guid:{}, ctmilli:{}", guid, System.currentTimeMillis());

        SourceRecordKafka record = new SourceRecordKafka(key, value, topic, partition, offset, msgts, guid);

        LOG.debug("guid:{}, ctmilli:{}", guid, System.currentTimeMillis());
        return record;
    }

    @Override
    public boolean isEndOfStream(SourceRecordKafka record) {
        return false;
    }

    @Override
    public TypeInformation<SourceRecordKafka> getProducedType() {
        return TypeInformation.of(SourceRecordKafka.class);
    }

}
