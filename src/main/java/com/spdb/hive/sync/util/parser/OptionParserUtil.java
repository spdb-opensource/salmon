package com.spdb.hive.sync.util.parser;

import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.commons.cli.*;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * project：hive-sync-1.1
 * package：com.spdb.hive.sync.util.parser
 * author：zhouxh
 * time：2021-06-10 16:47
 * description：
 */
public class OptionParserUtil {
    private final static Logger logger = LogManager.getLogger(OptionParserUtil.class);

    public static int optionParse(String[] args) {
        try {
            //  定义参数选项
            Options options = new Options();
            //  同步策略，需要追加参数s或者m 单表-s single 多表-s multi
            options.addOption(new Option("s", true, "同步策略，单表同步-s single 多表同步-s multi"));
            //  极速模式-speed
            options.addOption(new Option("speed", false, "极速模式，-speed"));
            //  断点续传模式-resume main_task_id
            options.addOption(new Option("resume", true, "断点续传模式-resume main_task_id"));
            //  distcp时使用-update方式
            options.addOption(new Option("update", false, "distcp时使用-update方式"));
            //  distcp时使用-delete方式
            options.addOption(new Option("delete", false, "distcp时使用-delete方式"));
            //  distcp时使用-overwrite方式
            options.addOption(new Option("overwrite", false, "distcp时使用-overwrite方式"));
            //  distcp时使用的map个数，需要追加参数 -m 6
            options.addOption(new Option("maps", true, "distcp时使用的map个数，需要追加参数 -m 6"));
            //  distcp每个map的带宽配置，单位是MB/second，需要追加参数 -bandwidth 100
            options.addOption(new Option("bandwidth", true, "distcp每个map的带宽配置，单位是MB/second，需要追加参数 -bandwidth 100"));
            //  distcp时跳过crc检查
            options.addOption(new Option("skipcrccheck", false, "distcp时跳过crc检查，-skipcrccheck"));
            // 是否基于快照的主数据同步
            options.addOption(new Option("snapshot", true, "-snapshot false 使用不基于快照的主数据同步"));
            // hdfs同步策略
            options.addOption(new Option("dynamic", false, "-dynamic 使用动态分配策略"));
            //  手动kill数据同步进程
            options.addOption(new Option("kill", true, "kill数据同步进程 -kill main_task_id"));
            //  视图同步进程
            options.addOption(new Option("view", false, "视图同步进程 -view"));
            //  所有数据表处理完成后跳过文件一致性校验
            options.addOption(new Option("checksum", false, "所有数据表处理完成后跳过文件一致性校验，-checksum"));

            //  定义复杂参数解析 -Dkey=value
            /*Option option = Option.builder("D")
                    .argName("property=value")
                    .numberOfArgs(2)
                    .valueSeparator('=')
                    .build();*/
            //  定义复杂参数解析 -Dkey=value
            Option option = OptionBuilder.withArgName("property=value")
                    .hasArgs(2)
                    .withValueSeparator('=')
                    .create("D");

            options.addOption(option);

            //  输出用例
            //  HelpFormatter help = new HelpFormatter();
            //  help.printHelp("参数说明", options);

            //  创建命令行解析器
            //CommandLineParser parser = new DefaultParser();
            CommandLineParser parser = new PosixParser();
            //  将定义好参数解析方式和本次命令行参数传入解析器
            CommandLine cli = parser.parse(options, args);

            //  解析同步策略
            String strategy = cli.getOptionValue("s");
            if (!Constant.SYNC_STRATEGY_SINGLE.equalsIgnoreCase(strategy)
                    && !Constant.SYNC_STRATEGY_MULTI.equalsIgnoreCase(strategy)
                    && !cli.hasOption("resume")
                    && !cli.hasOption("view")
                    && !cli.hasOption("kill")) {
                logger.error("参数解析错误，同步策略参数输入有误，请重新输入，单表同步-s single 多表同步-s multi");
                return -1;
            } else if (strategy != null) {
                PropUtil.setProValue(Constant.SYNC_STRATEGY, strategy);
            }

            //  解析极速模式
            boolean speed = cli.hasOption("speed");
            if (speed) {
                PropUtil.setProValue(Constant.SYNC_MODE, Constant.SPEED_MODE);
            }
            //  解析视图模式
            boolean viewmodel = cli.hasOption("view");
            if (viewmodel) {
                PropUtil.setProValue(Constant.SYNC_MODE, Constant.VIEW_MODE);
            }
            //  解析断点续传
            if (cli.hasOption("resume")) {
                String resumeId = cli.getOptionValue("resume");
                if (resumeId != null && NumberUtils.isDigits(resumeId)) {
                    PropUtil.setProValue(Constant.SYNC_MODE, Constant.RESUME_MODE);
                    PropUtil.setProValue(Constant.RESUME_MODE_ID, resumeId);
                } else {
                    logger.error("参数解析错误，-resume 参数后请传递正确的主任务ID");
                    return -1;
                }
            }
            //  解析distcp同步方式
            boolean update = cli.hasOption("update");
            boolean overwrite = cli.hasOption("overwrite");
            boolean delete= cli.hasOption("delete");
            //如果同时传递了update和overwrite给出报错提示
            if (update && overwrite) {
                logger.error("参数解析错误，-update、-overwrite参数不可同时出现");
                return -1;
            }
            if (update) {
                PropUtil.setProValue(Constant.DISTCP_UPDATE, String.valueOf(update));
            } else if (overwrite) {
                PropUtil.setProValue(Constant.DISTCP_OVERWRITE, String.valueOf(overwrite));
            } else {
                //  如果update和overwrite参数都没给则默认使用update参数
                PropUtil.setProValue(Constant.DISTCP_UPDATE, String.valueOf(true));
            }
            if (delete) {
            PropUtil.setProValue(Constant.DISTCP_DELETE, String.valueOf(delete));
            }
            //  解析skipcrccheck
            boolean skipcrccheck = cli.hasOption("skipcrccheck");
            if (skipcrccheck) {
                PropUtil.setProValue(Constant.DISTCP_SKIPCRCCHECK, String.valueOf(skipcrccheck));
            }
            //  解析distcp map个数
            String mapsStr = cli.getOptionValue("maps", "3");
            if (mapsStr != null && NumberUtils.isDigits(mapsStr)) {
                PropUtil.setProValue(Constant.DISTCP_MAPS, mapsStr);
            } else {
                logger.error("参数解析错误，-m 参数后请传递整数类型");
                return -1;
            }

            //  解析bandwidth
            String bandwidthStr = cli.getOptionValue("bandwidth", "20");
            if (bandwidthStr != null && NumberUtils.isNumber(bandwidthStr)) {
                PropUtil.setProValue(Constant.DISTCP_BANDWIDTH, bandwidthStr);
            } else {
                logger.error("参数解析错误，-bandwidth 参数后请传递整数类型");
                return -1;
            }

            //  解析同步并发数
            String syncConcsStr = cli.getOptionProperties("D").getProperty("sync.concurrent", "3");
            if (syncConcsStr != null && NumberUtils.isDigits(syncConcsStr)) {
                PropUtil.setProValue(Constant.SYNC_CONCURRENT, syncConcsStr);
            } else {
                logger.error("参数解析错误，sync.concurrent参数请传递整数类型");
                return -1;
            }

            //  解析主数据同步重试次数
            String distcpRetryNumStr = cli.getOptionProperties("D").getProperty("distcp.retry.num", "3");
            if (distcpRetryNumStr != null && NumberUtils.isDigits(distcpRetryNumStr)) {
                PropUtil.setProValue(Constant.DISTCP_RETRY_NUM, distcpRetryNumStr);
            } else {
                logger.error("参数解析错误，distcp.retry.num参数请传递整数类型");
                return -1;
            }

            //  解析snapshot参数
            String snapshot = cli.getOptionValue("snapshot");
            if (snapshot != null) {
                if ("false".equalsIgnoreCase(snapshot) || "true".equalsIgnoreCase(snapshot)) {
                    PropUtil.setProValue(Constant.DISTCP_SNAPSHOT, snapshot);
                } else {
                    logger.error("参数解析错误，-snapshot 参数后请传递true或者false");
                    return -1;
                }
            } else {
                PropUtil.setProValue(Constant.DISTCP_SNAPSHOT, String.valueOf(true));
            }

            //  解析kill参数
            if (cli.hasOption("kill")) {
                String killId = cli.getOptionValue("kill");
                if (killId != null && NumberUtils.isDigits(killId)) {
                    PropUtil.setProValue(Constant.TASK_STATUS_KILL, killId);
                } else {
                    logger.error("参数解析错误，-kill 参数后请传递正确的主任务ID");
                    return -1;
                }
            }

//            //  解析checksum
            boolean checksum = cli.hasOption("checksum");
            if (checksum) {
                PropUtil.setProValue(Constant.CHECKSUM, String.valueOf(checksum));
            }

            boolean dynamic = cli.hasOption("dynamic");
            if (dynamic) {
                PropUtil.setProValue(Constant.DYNAMIC, String.valueOf(dynamic));
            }

            logger.info("命令行参数解析结果");
            PropUtil.printValues();
            return 1;
        } catch (Exception e) {
           e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        optionParse(args);
    }
}
