package at.bronzels.libcdcdwstr.flink.global;

import at.bronzels.libcdcdw.bean.MyLogContext;
import at.bronzels.libcdcdw.util.MyLog4j2;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;

public class MyParameterTool {
    static public MyLogContext getLogContext(RuntimeContext context) {
        // Get global parameters
        ParameterTool parameterTool = (ParameterTool) context.getExecutionConfig().getGlobalJobParameters();
        // Read parameter
        Long launchedMS = parameterTool.getLong(MyLog4j2.MARKER_NAME_COMMONAPP_launchedms);
        String appName = parameterTool.get(MyLog4j2.MARKER_NAME_COMMONAPP_appname);
        return new MyLogContext(launchedMS, appName);
    }
}
