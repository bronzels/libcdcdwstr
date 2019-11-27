package at.bronzels.libcdcdwstr.flink.source;

import at.bronzels.libcdcdwstr.bean.AbstractSourceRecordKafka;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractKafkaDeserializationSchema<M extends AbstractSourceRecordKafka> implements KeyedDeserializationSchema<M> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());
}
