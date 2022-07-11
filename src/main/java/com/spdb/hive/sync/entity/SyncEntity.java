package com.spdb.hive.sync.entity;

/**
 * project：hive-sync-1.1
 * package：com.spdb.hive.sync.entity
 * author：zhouxh
 * time：2021-05-13 13:28
 * description：
 */
public class SyncEntity {

    private String sourceCluster;

    private String sourceDB;

    private String sourceTB;

    private String targetCluster;

    private String targetDB;

    private String targetTB;

    public SyncEntity() {
    }

    public SyncEntity(String sourceCluster, String sourceDB, String sourceTB, String targetCluster, String targetDB, String targetTB) {
        this.sourceCluster = sourceCluster;
        this.sourceDB = sourceDB;
        this.sourceTB = sourceTB;
        this.targetCluster = targetCluster;
        this.targetDB = targetDB;
        this.targetTB = targetTB;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SyncEntity)) return false;

        SyncEntity that = (SyncEntity) o;

        if (getSourceCluster() != null ? !getSourceCluster().equals(that.getSourceCluster()) : that.getSourceCluster() != null)
            return false;
        if (getSourceDB() != null ? !getSourceDB().equals(that.getSourceDB()) : that.getSourceDB() != null)
            return false;
        if (getSourceTB() != null ? !getSourceTB().equals(that.getSourceTB()) : that.getSourceTB() != null)
            return false;
        if (getTargetCluster() != null ? !getTargetCluster().equals(that.getTargetCluster()) : that.getTargetCluster() != null)
            return false;
        if (getTargetDB() != null ? !getTargetDB().equals(that.getTargetDB()) : that.getTargetDB() != null)
            return false;
        return getTargetTB() != null ? getTargetTB().equals(that.getTargetTB()) : that.getTargetTB() == null;
    }

    @Override
    public int hashCode() {
        int result = getSourceCluster() != null ? getSourceCluster().hashCode() : 0;
        result = 31 * result + (getSourceDB() != null ? getSourceDB().hashCode() : 0);
        result = 31 * result + (getSourceTB() != null ? getSourceTB().hashCode() : 0);
        result = 31 * result + (getTargetCluster() != null ? getTargetCluster().hashCode() : 0);
        result = 31 * result + (getTargetDB() != null ? getTargetDB().hashCode() : 0);
        result = 31 * result + (getTargetTB() != null ? getTargetTB().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SyncEntity{" +
                "sourceCluster='" + sourceCluster + '\'' +
                ", sourceDB='" + sourceDB + '\'' +
                ", sourceTB='" + sourceTB + '\'' +
                ", targetCluster='" + targetCluster + '\'' +
                ", targetDB='" + targetDB + '\'' +
                ", targetTB='" + targetTB + '\'' +
                '}';
    }
}
