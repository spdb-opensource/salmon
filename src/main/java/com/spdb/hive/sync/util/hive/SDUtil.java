package com.spdb.hive.sync.util.hive;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

/**
 * project：hive-test
 * package：com.spdb.hive.replication.util.hive
 * author：zhouxh
 * time：2021-03-29 14:34
 * description：hive StorageDescriptor工具类
 */
public class SDUtil {

    public static int sdCompare(StorageDescriptor ssd, StorageDescriptor dsd) {
        int lastComparison = 0;

        //lastComparison = Boolean.valueOf(ssd.isSetCols()).compareTo(dsd.isSetCols());
        lastComparison = Boolean.compare(ssd.isSetCols(), dsd.isSetCols());
        if (lastComparison != 0) {
            return lastComparison;
        }
        //  列信息发生了变化，比如新增了字段或者修改了字段
        if (ssd.isSetCols()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getCols(), dsd.getCols());
            if (lastComparison != 0) {
                //  将源表的cols同步到目标表
                //dsd.setCols(ssd.getCols());
                return lastComparison;
            }
        }

        //lastComparison = Boolean.valueOf(ssd.isSetInputFormat()).compareTo(dsd.isSetInputFormat());
        lastComparison = Boolean.compare(ssd.isSetInputFormat(), dsd.isSetInputFormat());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetInputFormat()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getInputFormat(), dsd.getInputFormat());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(ssd.isSetOutputFormat()).compareTo(dsd.isSetOutputFormat());
        lastComparison = Boolean.compare(ssd.isSetOutputFormat(), dsd.isSetOutputFormat());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetOutputFormat()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getOutputFormat(), dsd.getOutputFormat());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(ssd.isSetCompressed()).compareTo(dsd.isSetCompressed());
        lastComparison = Boolean.compare(ssd.isSetCompressed(), dsd.isSetCompressed());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetCompressed()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.isCompressed(), dsd.isCompressed());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(ssd.isSetNumBuckets()).compareTo(dsd.isSetNumBuckets());
        lastComparison = Boolean.compare(ssd.isSetNumBuckets(), dsd.isSetNumBuckets());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetNumBuckets()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getNumBuckets(), dsd.getNumBuckets());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(ssd.isSetSerdeInfo()).compareTo(dsd.isSetSerdeInfo());
        lastComparison = Boolean.compare(ssd.isSetSerdeInfo(), dsd.isSetSerdeInfo());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetSerdeInfo()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getSerdeInfo(), dsd.getSerdeInfo());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(ssd.isSetBucketCols()).compareTo(dsd.isSetBucketCols());
        lastComparison = Boolean.compare(ssd.isSetBucketCols(), dsd.isSetBucketCols());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetBucketCols()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getBucketCols(), dsd.getBucketCols());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(ssd.isSetSortCols()).compareTo(dsd.isSetSortCols());
        lastComparison = Boolean.compare(ssd.isSetSortCols(), dsd.isSetSortCols());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetSortCols()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getSortCols(), dsd.getSortCols());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(ssd.isSetParameters()).compareTo(dsd.isSetParameters());
        lastComparison = Boolean.compare(ssd.isSetParameters(), dsd.isSetParameters());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetParameters()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getParameters(), dsd.getParameters());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(ssd.isSetSkewedInfo()).compareTo(dsd.isSetSkewedInfo());
        lastComparison = Boolean.compare(ssd.isSetSkewedInfo(), dsd.isSetSkewedInfo());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (ssd.isSetSkewedInfo()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.getSkewedInfo(), dsd.getSkewedInfo());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        if (ssd.isSetStoredAsSubDirectories()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(ssd.isStoredAsSubDirectories(), dsd.isStoredAsSubDirectories());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }
}
