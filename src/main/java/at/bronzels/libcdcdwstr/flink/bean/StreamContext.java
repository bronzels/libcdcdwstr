package at.bronzels.libcdcdwstr.flink.bean;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;


public class StreamContext {
    public StreamExecutionEnvironment env;
    public StreamTableEnvironment tableEnv;

    public StreamContext(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        this.env = env;
        this.tableEnv = tableEnv;
    }
}
