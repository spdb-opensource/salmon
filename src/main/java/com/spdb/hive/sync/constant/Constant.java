package com.spdb.hive.sync.constant;

/**
 * project：hive-sync-1.1
 * package：com.spdb.hive.sync.constant
 * author：zhouxh
 * time：2021-05-26 15:25
 * description：常量类
 */
public class Constant {

    //  数据同步并发数
    public static final String SYNC_CONCURRENT = "sync.concurrent";

    /**
     * 任务运行模式
     */
    public static final String SYNC_MODE = "mode";
    //  新任务模式
    public static final String NEW_MODE = "new";
    //  断点续传模式
    public static final String RESUME_MODE = "resume";
    public static final String RESUME_MODE_ID = "resume_id";
    //  极速模式
    public static final String SPEED_MODE = "speed";
    //  视图模式
    public static final String VIEW_MODE = "view";
    /**
     * 任务运行状态
     */
    //  任务运行状态-等待
    public static final String TASK_STATUS_WAIT = "wait";
    //  任务运行状态-正在运行
    public static final String TASK_STATUS_RUNNING = "running";
    //  任务运行状态-成功
    public static final String TASK_STATUS_SUCCESS = "success";
    //  任务运行状态-成功（yarn作业）
    public static final String TASK_STATUS_SUCCEEDED = "SUCCEEDED";
    //  任务运行状态-失败
    public static final String TASK_STATUS_FAIL = "fail";
    //  任务运行状态-警告
    public static final String TASK_STATUS_WARN = "warn";
    //  任务运行状态-完成
    public static final String TASK_STATUS_FINISH = "finish";
    //  任务运行状态-kill
    public static final String TASK_STATUS_KILL = "kill";
    //  视图主数据无需同步
    public static final String VIEW_MAINDATA_NDO = "viewNoDo";

    /**
     * 前后缀
     */
    //  前缀
    public static final String PREFIX = "prefix";
    //  后缀
    public static final String SUFFIX = "suffix";

    /**
     * 数据同步策略
     */
    //  同步策略
    public static final String SYNC_STRATEGY = "strategy";
    //  同步策略-单表同步
    public static final String SYNC_STRATEGY_SINGLE = "single";
    //  同步策略-多表同步
    public static final String SYNC_STRATEGY_MULTI = "multi";

    /**
     * distcp参数
     */
    //  distcp重试次数
    public static final String DISTCP_RETRY_NUM = "distcp.retry.num";
    //  使用-update方式进行distcp
    public static final String DISTCP_UPDATE = "distcp.update";
    //  使用-overwrite方式进行distcp
    public static final String DISTCP_OVERWRITE = "distcp.overwrite";
    //  使用-delete方式进行distcp
    public static final String DISTCP_DELETE = "distcp.delete";
    //  distcp maps个数
    public static final String DISTCP_MAPS = "distcp.maps";
    //  distcp nameservices配置文件路径，包含所有集群的nameservice解析
    public static final String DISTCP_NAMESERVICES_CONF = "distcp.nameservices.conf";
    //  distcp所提交的yarn集群配置文件路径，即yarn-site.xml配置文件路径
    public static final String DISTCP_YARN_CONF = "distcp.yarn.conf";
    //  distcp每个map的带宽配置，单位是MB/second， -bandwidth 100
    public static final String DISTCP_BANDWIDTH = "distcp.bandwidth";
    //  distcp时跳过crc检查
    public static final String DISTCP_SKIPCRCCHECK = "skipcrccheck";

    //  distcp时跳过crc检查
    public static final String DYNAMIC = "dynamic";
    //  快照
    public static final String DISTCP_SNAPSHOT = "snapshot";
    //  不基于快照的distcp
    public static final String DISTCP_NO_SNAPSHOT = "false";
    //
    public static final String CHECKSUM="checksum";
}
