package com.spdb.hive.sync.util.hive;

import org.apache.hadoop.hive.metastore.api.Partition;

/**
 * project：hive-test
 * package：com.spdb.hive.replication.util.hive
 * author：zhouxh
 * time：2021-04-02 10:48
 * description：
 */
public class PartitionUtil {

    public static int compareTo(Partition sPartition, Partition dPartition) {

        int lastComparison = 0;

        lastComparison = Boolean.compare(sPartition.isSetValues(), dPartition.isSetValues());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (sPartition.isSetValues()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(sPartition.getValues(), dPartition.getValues());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.compare(sPartition.isSetDbName(), dPartition.isSetDbName());
        if (lastComparison != 0) {
            return lastComparison;
        }
        lastComparison = Boolean.compare(sPartition.isSetTableName(), dPartition.isSetTableName());
        if (lastComparison != 0) {
            return lastComparison;
        }

        lastComparison = Boolean.compare(sPartition.isSetSd(), dPartition.isSetSd());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (sPartition.isSetSd()) {
            //  这里修改了StorageDescriptor的compareTo方法
            //lastComparison = org.apache.thrift.TBaseHelper.compareTo(sPartition.getSd(), dPartition.getSd());
            lastComparison = SDUtil.sdCompare(sPartition.getSd(), dPartition.getSd());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        lastComparison = Boolean.compare(sPartition.isSetParameters(), dPartition.isSetParameters());
        if (lastComparison != 0) {
            return lastComparison;
        }

        //  这里对partition的parameter比较重新处理，将统计信息部分过滤掉，不再比对统计信息
        //  totalSize numRows rawDataSize numFiles COLUMN_STATS_ACCURATE等
        if (sPartition.isSetParameters()) {
            //  lastComparison = org.apache.thrift.TBaseHelper.compareTo(sPartition.getParameters(), dPartition.getParameters());
            //  这里不再采用自带的compareTo
            //  使用重载后的compareTo对table的parameter进行比较
            lastComparison = TBaseHelperUtil.compareTo(sPartition.getParameters(), dPartition.getParameters());

            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

}
