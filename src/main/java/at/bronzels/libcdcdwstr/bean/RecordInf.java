package at.bronzels.libcdcdwstr.bean;

public interface RecordInf {
    String getMsgSource();
    String getGuid();
    String getData();
    long getMsgts();
    int getPartition();
    String getTopic();
}
