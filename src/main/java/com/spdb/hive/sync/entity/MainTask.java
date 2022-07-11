package com.spdb.hive.sync.entity;

/**
 * project：db-util
 * package：PACKAGE_NAME
 * author：zhouxh
 * time：2021-04-15 16:50
 * description：
 */
public class MainTask {

    //  自增主键
    private int id;
    //  数据同步的参数
    private String mainTaskArgs;
    //  源集群名称
    private String sourceCluster;
    //  目标集群名称
    private String targetCluster;
    //  主任务运行状态 running 运行中 finish 运行结束
    private String mainTaskStatus;
    //  主任务运行结果，这个结果受所有子任务运行结果影响，success 运行成功 warn 部分运行成功，警告状态 fail 运行失败
    private String mainTaskResult;
    //  主任务开始时间
    private String startTime;
    //  主任务结束时间
    private String stopTime;
    //  记录插入时间，数据库维护
    private String createTime;
    //  记录更新时间，数据库维护
    private String updateTime;

    public MainTask() {
    }

    public MainTask(String mainTaskArgs, String sourceCluster,String targetCluster, String mainTaskStatus) {
        this.mainTaskArgs = mainTaskArgs;
        this.sourceCluster=sourceCluster;
        this.targetCluster = targetCluster;
        this.mainTaskStatus = mainTaskStatus;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMainTaskArgs() {
        return mainTaskArgs;
    }

    public void setMainTaskArgs(String mainTaskArgs) {
        this.mainTaskArgs = mainTaskArgs;
    }

    public String getSourceCluster() {
        return sourceCluster;
    }

    public void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    public String getTargetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
    }

    public String getMainTaskStatus() {
        return mainTaskStatus;
    }

    public void setMainTaskStatus(String mainTaskStatus) {
        this.mainTaskStatus = mainTaskStatus;
    }

    public String getMainTaskResult() {
        return mainTaskResult;
    }

    public void setMainTaskResult(String mainTaskResult) {
        this.mainTaskResult = mainTaskResult;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getStopTime() {
        return stopTime;
    }

    public void setStopTime(String stopTime) {
        this.stopTime = stopTime;
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

    @Override
    public String toString() {
        return "MainTask{" +
                "id=" + id +
                ", mainTaskArgs='" + mainTaskArgs + '\'' +
                ", sourceCluster='" + sourceCluster + '\'' +
                ", targetCluster='" + targetCluster + '\'' +
                ", mainTaskStatus='" + mainTaskStatus + '\'' +
                ", mainTaskResult='" + mainTaskResult + '\'' +
                ", startTime='" + startTime + '\'' +
                ", stopTime='" + stopTime + '\'' +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                '}';
    }
}
