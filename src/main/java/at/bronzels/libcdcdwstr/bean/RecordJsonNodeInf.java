package at.bronzels.libcdcdwstr.bean;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public interface RecordJsonNodeInf {
    String getMsgSource();
    String getGuid();
    JsonNode getData();
    long getMsgts();
    int getPartition();
    String getTopic();
}
