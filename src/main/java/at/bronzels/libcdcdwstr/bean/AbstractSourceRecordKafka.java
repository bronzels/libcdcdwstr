package at.bronzels.libcdcdwstr.bean;

import java.io.Serializable;

public class AbstractSourceRecordKafka<T> implements Serializable {
    private String key;
    private T value;
    private String topic;
    private int partition;
    private long offset;

    private long msgts;
    String guid;

    public AbstractSourceRecordKafka(String key, T value, String topic, int partition, long offset, long msgts, String guid) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.msgts = msgts;
        this.guid = guid;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setMsgts(long msgts) {
        this.msgts = msgts;
    }

    public long getMsgts() {
        return msgts;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }


    public String getGuid() {
        return guid;
    }

    public String getMsgSource() {
        return topic;
    }

    public T getData() {
        return value;
    }

}
