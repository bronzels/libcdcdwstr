package at.bronzels.libcdcdwstr.bean;

public class SourceRecordKafka extends AbstractSourceRecordKafka<String> implements RecordInf {

    public SourceRecordKafka(String key, String value, String topic, int partition, long offset, long msgts, String guid) {
        super(key, value, topic, partition, offset, msgts, guid);
    }
}
