package com.spdb.hive.sync.entity;

/**
 * project：db-util
 * package：PACKAGE_NAME
 * author：zhouxh
 * time：2021-04-15 16:55
 * description：
 */
public class SubTask {

    //  自增主键
    private int id;
    //  源集群名称
    private String sourceCluster;
    //  源数据库
    private String sourceDB;
    //  源数据表
    private String sourceTB;
    //  目标集群名称
    private String targetCluster;
    //  目标数据库
    private String targetDB;
    //  目标数据表
    private String targetTB;
    //  元数据同步状态wait 等待同步 success 同步成功 fail 同步失败，默认值wait
    private String metadataSyncStatus;
    //  元数据同步重试次数【预留字段】
    private int metadataSyncRetry;
    //  主数据同步状态wait 等待同步 success 同步成功 fail 同步失败，默认值wait
    private String maindataSyncStatus;
    //  源集群表的location
    private String maindataSyncSourcePath;
    //  目标集群表的location
    private String maindataSyncTargetPath;
    //  distcp作业参数
    private String maindataSyncArgs;
    //  主数据同步触发distcp后生成的MR的id
    private String maindataSyncJobId;
    //  MR作业运行状态，这里是直接采用的JobStatus.State自身的状态值RUNNING/SUCCEEDED/FAILED/PREP/KILLED
    private String maindataSyncJobStatus;
    //  主数据distcp同步的重试次数
    private int maindataSyncRetry;
    //  主数据同步开始时间
    private String maindataSyncStart;
    //  主数据同步结束时间
    private String maindataSyncStop;
    //  主任务表的id
    private int mainTaskId;
    //  记录插入时间，数据库维护
    private String createTime;
    //  记录更新时间，数据库维护
    private String updateTime;
    //  子任务状态
    private String subTaskStatus;

    public SubTask() {
    }

    public SubTask(
            String sourceCluster,
            String sourceDB,
            String sourceTB,
            String targetCluster,
            String targetDB,
            String targetTB,
            String metadataSyncStatus,
            int metadataSyncRetry,
            String maindataSyncStatus,
            String maindataSyncSourcePath,
            String maindataSyncTargetPath,
            String maindataSyncArgs,
            int maindataSyncRetry,
            int mainTaskId) {
        this.sourceCluster = sourceCluster;
        this.sourceDB = sourceDB;
        this.sourceTB = sourceTB;
        this.targetCluster = targetCluster;
        this.targetDB = targetDB;
        this.targetTB = targetTB;
        this.metadataSyncStatus = metadataSyncStatus;
        this.metadataSyncRetry = metadataSyncRetry;
        this.maindataSyncStatus = maindataSyncStatus;
        this.maindataSyncSourcePath = maindataSyncSourcePath;
        this.maindataSyncTargetPath = maindataSyncTargetPath;
        this.maindataSyncArgs = maindataSyncArgs;
        this.maindataSyncRetry = maindataSyncRetry;
        this.mainTaskId = mainTaskId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getSourceDB() {
        return sourceDB;
    }

    public void setSourceDB(String sourceDB) {
        this.sourceDB = sourceDB;
    }

    public String getSourceTB() {
        return sourceTB;
    }

    public void setSourceTB(String sourceTB) {
        this.sourceTB = sourceTB;
    }

    public String getSourceCluster() {
        return sourceCluster;
    }

    public void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    public String getTargetDB() {
        return targetDB;
    }

    public void setTargetDB(String targetDB) {
        this.targetDB = targetDB;
    }

    public String getTargetTB() {
        return targetTB;
    }

    public void setTargetTB(String targetTB) {
        this.targetTB = targetTB;
    }

    public String getTargetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
    }

    public String getMetadataSyncStatus() {
        return metadataSyncStatus;
    }

    public void setMetadataSyncStatus(String metadataSyncStatus) {
        this.metadataSyncStatus = metadataSyncStatus;
    }

    public int getMetadataSyncRetry() {
        return metadataSyncRetry;
    }

    public void setMetadataSyncRetry(int metadataSyncRetry) {
        this.metadataSyncRetry = metadataSyncRetry;
    }

    public String getMaindataSyncStatus() {
        return maindataSyncStatus;
    }

    public void setMaindataSyncStatus(String maindataSyncStatus) {
        this.maindataSyncStatus = maindataSyncStatus;
    }

    public String getMaindataSyncSourcePath() {
        return maindataSyncSourcePath;
    }

    public void setMaindataSyncSourcePath(String maindataSyncSourcePath) {
        this.maindataSyncSourcePath = maindataSyncSourcePath;
    }

    public String getMaindataSyncTargetPath() {
        return maindataSyncTargetPath;
    }

    public void setMaindataSyncTargetPath(String maindataSyncTargetPath) {
        this.maindataSyncTargetPath = maindataSyncTargetPath;
    }

    public String getMaindataSyncArgs() {
        return maindataSyncArgs;
    }

    public void setMaindataSyncArgs(String maindataSyncArgs) {
        this.maindataSyncArgs = maindataSyncArgs;
    }

    public String getMaindataSyncJobId() {
        return maindataSyncJobId;
    }

    public void setMaindataSyncJobId(String maindataSyncJobId) {
        this.maindataSyncJobId = maindataSyncJobId;
    }

    public String getMaindataSyncJobStatus() {
        return maindataSyncJobStatus;
    }

    public void setMaindataSyncJobStatus(String maindataSyncJobStatus) {
        this.maindataSyncJobStatus = maindataSyncJobStatus;
    }

    public int getMaindataSyncRetry() {
        return maindataSyncRetry;
    }

    public void setMaindataSyncRetry(int maindataSyncRetry) {
        this.maindataSyncRetry = maindataSyncRetry;
    }

    public String getMaindataSyncStart() {
        return maindataSyncStart;
    }

    public void setMaindataSyncStart(String maindataSyncStart) {
        this.maindataSyncStart = maindataSyncStart;
    }

    public String getMaindataSyncStop() {
        return maindataSyncStop;
    }

    public void setMaindataSyncStop(String maindataSyncStop) {
        this.maindataSyncStop = maindataSyncStop;
    }

    public int getMainTaskId() {
        return mainTaskId;
    }

    public void setMainTaskId(int mainTaskId) {
        this.mainTaskId = mainTaskId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getSubTaskStatus() {
        return subTaskStatus;
    }

    public void setSubTaskStatus(String subTaskStatus) {
        this.subTaskStatus = subTaskStatus;
    }

    public String getBriefInfo() {
        return "ID: " + id + " " + sourceCluster + '.' + sourceDB + '.' + sourceTB +
                '[' + targetCluster + '.' + targetDB + "." + targetTB + ']';
    }

}
