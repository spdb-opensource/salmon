package com.spdb.hive.sync.util.hive;

import com.spdb.hive.sync.util.auth.Authkrb5;
import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MyHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * project：hive-test
 * package：com.spdb.hive.replication.util
 * author：zhouxh
 * time：2021-03-29 14:01
 * description：hivemetastoreclient工具类
 */
public class HMSCUtil {

    // Logger和LoggerFactory导入的是org.slf4j包
    private final static Logger logger = LogManager.getLogger(HMSCUtil.class);

    /**
     * 输出表详情信息
     *
     * @param clusterConfPath 集群配置文件路径
     * @param dbName          数据库名称
     * @param tableName       数据表名称
     */
    public static void descFormatTable(String clusterConfPath, String dbName, String tableName) {
        try {

            //  创建集群hivemetastoreclient客户端对象
            MyHiveMetaStoreClient shmsc = getHMSC(clusterConfPath);
            //  获得目标数据表对象
            Table table = shmsc.getTable(dbName, tableName);

            //  获得表的storageDescriptor
            StorageDescriptor sd = table.getSd();

            System.out.println("=====================================================");

            //  获得表所有字段
            List<FieldSchema> cols = sd.getCols();

            System.out.println("# col_name\tdata_type\tcomment ");
            for (FieldSchema fs : cols) {
                System.out.println(fs.getName() + "\t" + fs.getType() + "\t" + fs.getComment());
            }

            //  获得表的分区信息
            System.out.println("\r\n# Partition Information  ");
            System.out.println("# col_name\tdata_type\tcomment ");
            for (FieldSchema fs : table.getPartitionKeys()) {
                System.out.println(fs.getName() + "\t" + fs.getType() + "\t" + fs.getComment());
            }

            System.out.println("#分区详情");
            List<Partition> partitions = shmsc.listPartitions(dbName, tableName, Short.MAX_VALUE);

            for (Partition partition : partitions) {

                System.out.println(partition.getValues());
                Map<String, String> parParameters = partition.getParameters();

                Set<String> tableKeys = parParameters.keySet();
                for (String key : tableKeys) {
                    System.out.println("\t" + key + "\t" + parParameters.get(key));
                }

            }

            //  获得表的明细信息
            System.out.println("\r\n# Detailed Table Information");

            System.out.println("Database:\t" + table.getDbName());
            System.out.println("OwnerType:\t" + table.getOwnerType());
            System.out.println("Owner:\t" + table.getOwner());
            System.out.println("CreateTime:\t" + table.getCreateTime());
            System.out.println("LastAccessTime:\t" + table.getLastAccessTime());
            System.out.println("Retention:\t" + table.getRetention());
            System.out.println("Location:\t" + sd.getLocation());
            System.out.println("Table Type:\t" + table.getTableType());
            System.out.println("Table Parameters:");

            Map<String, String> tableParameters = table.getParameters();
            Set<String> tableKeys = tableParameters.keySet();
            for (String key : tableKeys) {
                System.out.println("\t" + key + "\t" + tableParameters.get(key));
            }

            System.out.println("\r\n# Storage Information ");
            System.out.println("SerDe Library:\t" + sd.getSerdeInfo().getSerializationLib());
            System.out.println("InputFormat:\t" + sd.getInputFormat());
            System.out.println("OutputFormat:\t" + sd.getOutputFormat());
            System.out.println("Compressed:\t" + sd.isCompressed());
            System.out.println("Num Buckets:\t" + sd.getBucketCols().size());
            System.out.println("Bucket Columns:\t" + sd.getBucketCols());
            System.out.println("Sort Columns:\t" + sd.getSortCols());

            System.out.println("Storage Desc Params:");

            Map<String, String> sdParameters = sd.getSerdeInfo().getParameters();
            Set<String> sdKeys = sdParameters.keySet();

            for (String key : sdKeys) {
                System.out.println("\t" + key + "\t" + sdParameters.get(key));
            }
        } catch (TException e) {
            e.printStackTrace();
        }

    }

    /**
     * 获得集群hivemetastoreclient客户端对象
     *
     * @param clusterConfPath hive-site.xml配置文件路径
     * @return 对应集群的HiveMetaStoreClient对象
     */
    public static MyHiveMetaStoreClient getHMSC(String clusterConfPath) {
        try {
            //  创建集群配置对象
            HiveConf hc = new HiveConf();

            String hiveConf = PropUtil.getProValue(clusterConfPath);

            if (hiveConf == null) {
                logger.error("集群配置文件加载失败，程序退出" + clusterConfPath);
                System.exit(1);
            }else{
                logger.info("集群配置文件加载成功，路径为：" + hiveConf+":"+clusterConfPath);
            }

            //  加载配置文件
            hc.addResource(hiveConf);

            //  创建hivemetastoreclient客户端对象
            MyHiveMetaStoreClient hmsc = new MyHiveMetaStoreClient(hc);

            return hmsc;
        } catch (MetaException metaException) {
            metaException.printStackTrace();
        }

        return null;
    }

    /**
     * 判断数据表是否存在
     *
     * @param clusterConfPath 集群配置文件路径
     * @param dbName          数据库名称
     * @param tableName       数据表名称
     * @return true 表存在 false 表不存在
     */
    public static boolean isExist(String clusterConfPath, String dbName, String tableName) {

        try {

            //  创建源集群hivemetastoreclient客户端对象
            MyHiveMetaStoreClient shmsc = getHMSC(clusterConfPath);
            //  首先检测源集群表是否存在
            return shmsc.tableExists(dbName, tableName);
        } catch (TException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void close(MyHiveMetaStoreClient hmsc) {
        if (hmsc != null) {
            hmsc.close();
        }
    }

    public static void main(String[] args) throws TException {
        System.out.println(getHMSC("CDH6_BDS_UAT.hive.conf"));
    }
}
