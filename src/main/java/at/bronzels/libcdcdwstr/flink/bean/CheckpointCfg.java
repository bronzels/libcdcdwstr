package at.bronzels.libcdcdwstr.flink.bean;

public class CheckpointCfg {
}
/*
//设置statebackend
env.setStateBackend(new MemoryStateBackend());

CheckpointConfig config = env.getCheckpointConfig();

// 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// 设置checkpoint的周期, 每隔1000 ms进行启动一个检查点
config.setCheckpointInterval(1000);

// 设置模式为exactly-once
config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
config.setMinPauseBetweenCheckpoints(500);
// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
config.setCheckpointTimeout(60000);
// 同一时间只允许进行一个检查点
config.setMaxConcurrentCheckpoints(1);
 */
