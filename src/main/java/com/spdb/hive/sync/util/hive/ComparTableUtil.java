package com.spdb.hive.sync.util.hive;

import com.spdb.hive.sync.service.HMSCService;
import com.spdb.hive.sync.start.Start;
import com.spdb.hive.sync.util.auth.Authkrb5;
import org.apache.hadoop.hive.metastore.MyHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import com.spdb.hive.sync.util.LogUtil;

/**
 * project：hive-test
 * package：com.spdb.hive.replication.start
 * author：zhouxh
 * time：2021-03-30 9:28
 * description：启动入口
 */
public class ComparTableUtil {

    // Logger和LoggerFactory导入的是org.slf4j包
    private final static Logger logger = LogUtil.getLogger();

    /**
     * @param desCluster_S 目标集群名称，这里的名称取的是cluster.properties中的值
     *                   集群名称拼接.hive.conf形成对应集群hive-site.xml配置文件路径
     *                   集群名称拼接.hdfs.rpc形成对应集群hive数据存储的hdfs路径中间部分字符串
     * @param dbName_S     需要同步的数据表所在数据库
     * @param tableName_S  需要同步的数据表
     * @param desCluster_D 目标集群名称，这里的名称取的是cluster.properties中的值
     *                   集群名称拼接.hive.conf形成对应集群hive-site.xml配置文件路径
     *                   集群名称拼接.hdfs.rpc形成对应集群hive数据存储的hdfs路径中间部分字符串
     * @param dbName_D     需要同步的数据表所在数据库
     * @param tableName_D  需要同步的数据表
     *
     *                     内表比较
     */
    public String comparTable(String desCluster_S, String dbName_S, String tableName_S,String desCluster_D, String dbName_D, String tableName_D) {
            String compareResult = "";
        try {
            //  获得源集群的HMSC对象
            MyHiveMetaStoreClient dhmsc = HMSCUtil.getHMSC( "cdh5.hive.conf");
            System.out.println(dhmsc.tableExists(dbName_D, tableName_D));
            if  (dhmsc.tableExists(dbName_D, tableName_D)==false){
                compareResult="源表不存在！无法比较";
                return compareResult;
            }
            //  获得目标集群的HMSC对象
            MyHiveMetaStoreClient shmsc = HMSCUtil.getHMSC("cdh5.hive.conf");


            if (shmsc.tableExists(dbName_S, tableName_S)==false) {
                compareResult="目标表不存在！无法比较";
                return compareResult;
            }

            Table sTable = shmsc.getTable(dbName_S, tableName_S);
            Table dTable = shmsc.getTable(dbName_S, tableName_S);

            //判断是否是相同的表类型：内表和外表
            String StableType=sTable.getTableType();
            String DtableType=dTable.getTableType();

            if (StableType.equals(DtableType)){
                System.out.println("两个表类型都为："+StableType+",开始比较表结构！");
            }else{
                compareResult="表类型不一致:源表为"+StableType+"，目标表为："+DtableType;
                return compareResult;
            }

            //1、判断源表是否是分区表
            boolean isPartitionTableS =new HMSCService().isPartitionTable(sTable);
            boolean isPartitionTableD =new HMSCService().isPartitionTable(sTable);
            List<Partition> sPartitions = new ArrayList<Partition>();
            if (isPartitionTableD==true && isPartitionTableS==true){
                //  如果表为分区表且分区个数超过32767则给出WARN提示并跳过该表
                //  获得源表分区个数

                sPartitions = shmsc.listPartitions(dbName_S, tableName_S, Short.MAX_VALUE);

                if (sPartitions.size() >= Short.MAX_VALUE) {
                    logger.warn("********源表分区数过大，跳过该表" + dbName_S + "." + tableName_S + "********");
                    return "分区数过大，分区表比较失败";
                }
                //2、分区表比较
                if (comparTableWithPartition(sTable,dTable,dhmsc,sPartitions)){
                    return "分区表，元数据比较后一致";
                }else{
                    return "分区表，元数据比较后不一致";
                }
            }else if(isPartitionTableD==false && isPartitionTableS==false){
                //3、非分区表比较

                if (comparTableWithNoPartition(sTable,dTable)){
                    return "非分区表，元数据比较后一致";
                }else{
                    return "非分区表，元数据比较后不一致";
                }
            }else{
                compareResult="源表和目标表不都是分区表，无法比较";
                return compareResult;
            }


        } catch (TException e) {
            e.printStackTrace();
        }
        return "比较结束！";
    }

//    分区表的比较方法
    public Boolean comparTableWithPartition(Table sTable, Table dTable, MyHiveMetaStoreClient dhmsc, List<Partition> sPartitions) {

        //  获得数据库名
        String dbName_D = sTable.getDbName();
        //  获得表名
        String tableName_D = sTable.getTableName();

        try {
            //  表级别的元数据比较
            if (!comparTableWithNoPartition( sTable,dTable)){
                return false;
            }

            //  获得目标表分区个数
            List<Partition> dPartitions = dhmsc.listPartitions(dbName_D, tableName_D, Short.MAX_VALUE);

            //  将源分区和目标分区进行转换封装Map<partition.getValues().toString(),partition>，便于进行源和目标的比较
            HashMap<String, Partition> sMapPartitions = new HMSCService().transforPartitions(sPartitions);
            HashMap<String, Partition> dMapPartitions = new HMSCService().transforPartitions(dPartitions);

            //  获得源分区和目标分区所有分区值集合
            Set<String> sPartitionKeys = sMapPartitions.keySet();
            Set<String> dPartitionKeys = dMapPartitions.keySet();

            //  如果源分区个数!=目标分区个数，说明源分区有过删除，则直接给出WARN提示并跳过该表
            if (sMapPartitions.size() != dMapPartitions.size()) {
                logger.warn("********两张表分区数目不一致！********");
                return false;
            }

            //  遍历源分区
            for (String sKey : sPartitionKeys) {

                //  判断目标分区是否存在该分区
                if (dPartitionKeys.contains(sKey)) {
                    //  目标分区存在该分区，继续比较partition的compareTo
                    int i = PartitionUtil.compareTo(sMapPartitions.get(sKey), dMapPartitions.get(sKey));

                    if (i != 0) {
                        //  分区发生变化
                        logger.info("分区发生变化，不一致");
                        return false;
                    }

                } else {
                    //  目标分区不存在该分区，根据源分区创建新分区
                    return false;
                }
            }

        } catch (TException e) {
            e.printStackTrace();
        }

        return true;
}
//非分区表的比较方法
    public Boolean comparTableWithNoPartition(Table sTable, Table dTable) {

        //  获得数据库名
        String dbName_S = sTable.getDbName();
        //  获得表名
        String tableName_S = sTable.getTableName();
        //  获得数据库名
        String dbName_D = sTable.getDbName();
        //  获得表名
        String tableName_D = sTable.getTableName();

        logger.info("表1" + dbName_S + "." + tableName_S +"表2"+dbName_D+"."+tableName_D+ "开始比对元数据");


        //  对源集群和目标集群的数据表进行元数据比对
        int comparison = TableUtil.tableCompare(sTable, dTable);
        //  比对结果不为0说明源的结构发生了变化
        if (comparison != 0) {
            logger.info("两表元数据不一致");
            return  false;
        } else {
            logger.info("两表元数据一致");
            return  true;
        }
    }
    public static void main(String[] args) {

        Authkrb5.authkrb5Default();

//        原集群库表
        String desCluster_S= "dongcha";
        String dbName_S="distcp2";
        String tableName_S="student_m_np1";
//        目标集群库表
        String desCluster_D= "dongcha";
        String dbName_D="distcp_bdr";
        String tableName_D="student_m_np1";

        ComparTableUtil ct = new ComparTableUtil();

        System.out.println(ct.comparTable(desCluster_S,dbName_S,tableName_S,desCluster_D,dbName_D,tableName_D));
    }
}
