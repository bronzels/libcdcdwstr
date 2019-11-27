package at.bronzels.libcdcdwstr;

import at.bronzels.libcdcdw.CommonCli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.*;

public abstract class SimpleCommonCliInput extends CommonCli {
    String cdcPrefix;// = "betamysqldeb";

    protected String inputPrefix;// = "";
    protected String outputPrefix;// = "";

    boolean flinkInputLocalMode = false;
    Integer flinkInputParallelism4Local = null;

    protected String strNameCliInput = null;

    boolean isOperatorChainDisabled = false;

    int timeZoneHourOffset = +8;

    public String getCdcPrefix() {
        return cdcPrefix;
    }

    public String getInputPrefix() {
        return inputPrefix;
    }

    public String getOutputPrefix() {
        return outputPrefix;
    }

    public Integer getFlinkInputParallelism4Local() {
        return flinkInputParallelism4Local;
    }

    public boolean isFlinkInputLocalMode() {
        return flinkInputLocalMode;
    }

    public boolean isOperatorChainDisabled() {
        return isOperatorChainDisabled;
    }

    public int getTimeZoneHourOffset() {
        return timeZoneHourOffset;
    }

    public String getStrNameCliInput() {
        return strNameCliInput;
    }

    public SimpleCommonCliInput() {
    }

    @Override
    public Options buildOptions() {
        Options options = super.buildOptions();
        //短选项，长选项，选项后是否有参数，描述
        //帮助
        Option option;

        option = new Option("h", "help", false, "display help text");
        options.addOption(option);

        //cdc消息源前缀
        option = new Option("c", "cdc", true, "CDC messaging topic input prefixWithPrj");
        options.addOption(option);

        //输入前缀
        option = new Option("i", "input", true, "input prefixWithPrj to have multiple runnings for same project");
        options.addOption(option);

        //输出前缀
        option = new Option("o", "output", true, "output prefixWithPrj to have multiple runnings for same project");
        options.addOption(option);

        //flink input
        option = new Option("f", "flinkinput", true,
                String.format("flink input(now only parallelism_to_set to run in local mode)"));
        options.addOption(option);

        //disable operator chain
        option = new Option("dc", "disableoc", false,
                String.format("disable operator chain"));
        options.addOption(option);

        //time zone offset
        option = new Option("tz", "timezoneoffset", true,
                String.format("time zone offset of CDC source DB located"));
        options.addOption(option);

        return options;
    }

    @Override
    public boolean parseIsHelp(Options options, String[] args) {
        if(super.parseIsHelp(options, args))
            return true;

        strNameCliInput = String.join(at.bronzels.libcdcdw.Constants.commonSep, Arrays.asList(args));
        CommandLine comm = getCommandLine(options, args);

        if (comm.hasOption("c"))//betamysqldeb.
            cdcPrefix = comm.getOptionValue('c');
        if (comm.hasOption("i"))
            inputPrefix = comm.getOptionValue('i');
        if (comm.hasOption("o"))
            outputPrefix = comm.getOptionValue('o');

        if (comm.hasOption("f")) {
            String flinkInput = comm.getOptionValue('f');
            String[] flinkInputArr = flinkInput.split(",", -1);
            if(flinkInputArr.length != 1) throw new RuntimeException("flink input now only for parallelism in local mode to set");
            flinkInputLocalMode = true;
            flinkInputParallelism4Local = Integer.parseInt(flinkInputArr[0]);
        }
        if (comm.hasOption("dc")) {
            isOperatorChainDisabled = true;
        }
        if (comm.hasOption("tz")) {
            String timeZoneStr = comm.getOptionValue("tz");
            if(!NumberUtils.isNumber(timeZoneStr)) throw new RuntimeException("-tz input has to be a number");
            timeZoneHourOffset = Integer.parseInt(timeZoneStr);
        }

        return false;
    }

}
