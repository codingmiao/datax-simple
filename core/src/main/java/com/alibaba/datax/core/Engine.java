package com.alibaba.datax.core;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowtools.common.utils.AsyncTaskUtil;
import org.wowtools.common.utils.ResourcesReader;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine {
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    private static final AtomicLong jobIdIndex = new AtomicLong(1);

    /* check job model (job/task) first */
    public void start(Configuration allConf) {

        // 绑定column转换信息
        ColumnCast.bind(allConf);
        /**
         * 初始化PluginLoader，可以获取各种插件配置
         */
        LoadUtil.bind(allConf);

        boolean isJob = !("taskGroup".equalsIgnoreCase(allConf
                .getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));
        //JobContainer会在schedule后再行进行设置和调整值
        int channelNumber = 0;
        AbstractContainer container;
        long instanceId;
        int taskGroupId = -1;
        if (isJob) {
            allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, null);
            container = new JobContainer(allConf);
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);

        } else {
            container = new TaskGroupContainer(allConf);
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
            taskGroupId = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            channelNumber = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
        }

        //缺省打开perfTrace
        boolean traceEnable = allConf.getBool(CoreConstant.DATAX_CORE_CONTAINER_TRACE_ENABLE, true);
        boolean perfReportEnable = allConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_PERFLOG, true);

        //standlone模式的datax shell任务不进行汇报
        if (instanceId == -1) {
            perfReportEnable = false;
        }

        int priority = 0;
        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        } catch (NumberFormatException e) {
            LOG.warn("prioriy set to 0, because NumberFormatException, the value is: " + System.getProperty("PROIORY"));
        }

        Configuration jobInfoConfig = allConf.getConfiguration(CoreConstant.DATAX_JOB_JOBINFO);
        //初始化PerfTrace
        PerfTrace perfTrace = PerfTrace.getInstance(isJob, instanceId, taskGroupId, priority, traceEnable);
        perfTrace.setJobInfo(jobInfoConfig, perfReportEnable, channelNumber);
        container.start();

    }


    // 注意屏蔽敏感信息
    public static String filterJobConfiguration(final Configuration configuration) {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();

        Configuration jobContent = jobConfWithSetting.getConfiguration("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content", jobContent);

        return jobConfWithSetting.beautify();
    }

    public static Configuration filterSensitiveConfiguration(Configuration configuration) {
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }
        return configuration;
    }

    public static void entry(String jobPath) throws Throwable {
        LOG.info("执行配置文件任务:{}", jobPath);

        Options options = new Options();
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("mode", true, "Job runtime mode.");

        Configuration configuration = ConfigParser.parse(jobPath);

        long jobId = jobIdIndex.addAndGet(1);

        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);

        //打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            LOG.info(vmInfo.toString());
        }

        LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");

        LOG.debug(configuration.toJSON());

        ConfigurationValidate.doValidate(configuration);
        Engine engine = new Engine();
        engine.start(configuration);
    }


    public static void main(String[] args) {
        boolean success = execute();
        System.exit(success ? 0 : 1);
    }

    private static boolean execute() {
        boolean success = true;
        Config config = parseTasks();
        for (List<String> task : config.taskJsons) {
            for (String jobPath : task) {
                try {
                    entry(jobPath);
                } catch (Throwable throwable) {
                    success = false;
                    err(throwable);
                    break;
                }
            }
            if (!success) {
                return false;
            }
        }
        LOG.info("datax执行完成");
        return true;
    }

    private static void err(Throwable e) {
        LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(e));

        if (e instanceof DataXException) {
            DataXException tempException = (DataXException) e;
            ErrorCode errorCode = tempException.getErrorCode();
            if (errorCode instanceof FrameworkErrorCode) {
                FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
            }
        }
    }

    private static Config parseTasks() {
        String[] rows = ResourcesReader.readStr(Engine.class, "/datax/tasks.md").split("\n");
        for (int i = 0; i < rows.length; i++) {
            rows[i] = rows[i].trim();
        }
        Config config = parseConfig(rows);

        List<List<String>> tasks = new LinkedList<>();
        List<String> threadTasks = null;
        boolean inTask = false;
        for (int i = config.configEnd + 1; i < rows.length; i++) {
            String row = rows[i];
            if (StringUtils.isBlank(row)) {//空行
                continue;
            }
            if (row.charAt(0) == '#') {//注释
                continue;
            }
            row = row.trim();
            if ("```".equals(row)) {//进出代码块
                if (inTask) {//即将离开代码块
                    tasks.add(threadTasks);
                } else {//即将进入代码块
                    threadTasks = new LinkedList<>();
                }
                inTask = !inTask;
                continue;
            }
            if (inTask) {//在代码块内则添加
                String path;
                if (row.charAt(0) == '<') {
                    int e = row.indexOf(">") + 1;
                    String pathHeadKey = row.substring(0, e);
                    String pathHead = config.pathHeads.get(pathHeadKey);
                    if (null == pathHead) {
                        throw new RuntimeException("相对路径 " + pathHeadKey + " 未指定:" + row);
                    }
                    path = pathHead + File.separator + row.substring(e);
                } else {
                    path = row;
                }
                threadTasks.add(path);
            }
        }
        config.taskJsons = tasks;
        return config;
    }

    private static Config parseConfig(String[] rows) {
        Config config = new Config();
        for (int i = 0; i < rows.length; i++) {
            if ("end config".equals(rows[i])) {
                config.configEnd = i;
            }
        }
        if (config.configEnd > 0) {
            HashSet<String> seted = new HashSet<>();//重复配置检查
            HashSet<String> pathheads = new HashSet<>();
            for (int i = 0; i < config.configEnd; i++) {
                String row = rows[i];
                if (StringUtils.isBlank(row)) {//空行
                    continue;
                }
                if (row.charAt(0) == '#') {//注释
                    continue;
                }
                String[] cfg = row.split("=", 2);
                if (2 != cfg.length) {
                    continue;
                }
                String key = cfg[0].trim();
                if (!seted.add(key)) {
                    throw new RuntimeException("重复的配置项:" + key);
                }
                String value = cfg[1].trim();
                switch (key) {
                    case "pathhead":
                        String[] heads = value.split(",");
                        for (String head : heads) {
                            pathheads.add(head);
                        }
                        break;
                    default:
                        if (pathheads.contains(key)) {
                            config.pathHeads.put("<" + key + ">", value);
                        } else {
                            throw new RuntimeException("无效的配置项:" + key);
                        }

                }

            }
        }
        return config;
    }

    private static final class Config {
        private HashMap<String, String> pathHeads = new HashMap<>();//相对路径头
        private int configEnd = -1;//配置结束于第几行
        private List<List<String>> taskJsons;
    }

}
