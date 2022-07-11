package com.spdb.hive.sync.entity;

/**
 * project：hive-sync-1.2
 * package：com.spdb.hive.sync.entity
 * author：zhouxh
 * time：2021-06-25 16:10
 * description：
 */
public class Snapshot {
    //  自增主键
    private int id;
    //  快照路径
    private String path;
    //  快照名称
    private String name;
    //  集群名称
    private String cluster;
    //  快照类型 源/目标
    private String type;
    //  主任务表的id
    private int mainTaskId;
    //  记录插入时间，数据库维护
    private String createTime;
    //  记录更新时间，数据库维护
    private String updateTime;
    //  删除标记 default 0 删除则更新为1
    private int deleteFlag;

    public Snapshot() {
    }

    public Snapshot(String path, String name, String cluster, String type, int mainTaskId) {
        this.path = path;
        this.name = name;
        this.cluster = cluster;
        this.type = type;
        this.mainTaskId = mainTaskId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public int getDeleteFlag() {
        return deleteFlag;
    }

    public void setDeleteFlag(int deleteFlag) {
        this.deleteFlag = deleteFlag;
    }

    @Override
    public String toString() {
        return "Snapshot{" +
                "id=" + id +
                ", path='" + path + '\'' +
                ", name='" + name + '\'' +
                ", cluster='" + cluster + '\'' +
                ", type='" + type + '\'' +
                ", mainTaskId=" + mainTaskId +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", deleteFlag=" + deleteFlag +
                '}';
    }
}
