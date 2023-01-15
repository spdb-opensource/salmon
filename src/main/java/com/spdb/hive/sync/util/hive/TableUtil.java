package com.spdb.hive.sync.util.hive;

import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.entity.SyncEntity;
import com.spdb.hive.sync.util.auth.Authkrb5;
import com.spdb.hive.sync.util.parser.OptionParserUtil;
import com.spdb.hive.sync.util.property.PropUtil;
//import com.sun.tools.internal.jxc.ap.Const;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.MyHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.spdb.hive.sync.util.LogUtil;
/**
 * project：hive-test
 * package：com.spdb.hive.replication.util.hive
 * author：zhouxh
 * time：2021-03-29 15:55
 * description：hive Table 工具类
 */
public class TableUtil {

    private final static Logger logger = LogUtil.getLogger();

    /**
     * 对table对象的元数据进行比较
     *
     * @param sTable 源表
     * @param dTable 目标表
     * @return 返回比较结果
     */
    public static int tableCompare(Table sTable, Table dTable) {
        int lastComparison = 0;

        //lastComparison = Boolean.valueOf(sTable.isSetTableName()).compareTo(dTable.isSetTableName());
        lastComparison = Boolean.compare(sTable.isSetTableName(), dTable.isSetTableName());
        if (lastComparison != 0) {
            return lastComparison;
        }
        //lastComparison = Boolean.valueOf(sTable.isSetDbName()).compareTo(dTable.isSetDbName());
        lastComparison = Boolean.compare(sTable.isSetDbName(), dTable.isSetDbName());
        if (lastComparison != 0) {
            return lastComparison;
        }

        //lastComparison = Boolean.valueOf(sTable.isSetSd()).compareTo(dTable.isSetSd());
        lastComparison = Boolean.compare(sTable.isSetSd(), dTable.isSetSd());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (sTable.isSetSd()) {
            //  这里修改了StorageDescriptor的compareTo方法
            lastComparison = SDUtil.sdCompare(sTable.getSd(), dTable.getSd());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(sTable.isSetPartitionKeys()).compareTo(dTable.isSetPartitionKeys());
        lastComparison = Boolean.compare(sTable.isSetPartitionKeys(), dTable.isSetPartitionKeys());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (sTable.isSetPartitionKeys()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(sTable.getPartitionKeys(), dTable.getPartitionKeys());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //  关于tableParameter的比较需要特别注意，即使在代码层面将目标表的parameters设置为何源表一致，但是最终持久化下来后还是不一致
        //  主要是因为alter的指令发送到metastore后会对表的数据做一些统计，然后将统计的结果更新到表的parameters中
        //lastComparison = Boolean.valueOf(sTable.isSetParameters()).compareTo(dTable.isSetParameters());
        lastComparison = Boolean.compare(sTable.isSetParameters(), dTable.isSetParameters());
        if (lastComparison != 0) {
            return lastComparison;
        }

        /**
         * 后期需要fix这里，如果仅仅是transient_lastDdlTime发生变化的话不需要刷新元数据，但是主数据需要同步
         */

        //  这里对table的parameter比较重新处理，将统计信息部分过滤掉，不再比对统计信息
        //  totalSize numRows rawDataSize numFiles COLUMN_STATS_ACCURATE等
        if (sTable.isSetParameters()) {
            //  这里不再采用自带的compareTo
            //  lastComparison = org.apache.thrift.TBaseHelper.compareTo(sTable.getParameters(), dTable.getParameters());
            //  使用重载后的compareTo对table的parameter进行比较
            lastComparison = TBaseHelperUtil.compareTo(sTable.getParameters(), dTable.getParameters());
            if (lastComparison != 0) {
                //  源表和目标表的table parameter不一致
                //dTable.setParameters(sTable.getParameters());
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(sTable.isSetViewOriginalText()).compareTo(dTable.isSetViewOriginalText());
        lastComparison = Boolean.compare(sTable.isSetViewOriginalText(), dTable.isSetViewOriginalText());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (sTable.isSetViewOriginalText()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(sTable.getViewOriginalText(), dTable.getViewOriginalText());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(sTable.isSetViewExpandedText()).compareTo(dTable.isSetViewExpandedText());
        lastComparison = Boolean.compare(sTable.isSetViewExpandedText(), dTable.isSetViewExpandedText());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (sTable.isSetViewExpandedText()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(sTable.getViewExpandedText(), dTable.getViewExpandedText());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        //lastComparison = Boolean.valueOf(sTable.isSetTableType()).compareTo(dTable.isSetTableType());
        lastComparison = Boolean.compare(sTable.isSetTableType(), dTable.isSetTableType());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (sTable.isSetTableType()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(sTable.getTableType(), dTable.getTableType());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        //lastComparison = Boolean.valueOf(sTable.isSetTemporary()).compareTo(dTable.isSetTemporary());
        lastComparison = Boolean.compare(sTable.isSetTemporary(), dTable.isSetTemporary());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (sTable.isSetTemporary()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(sTable.isTemporary(), sTable.isTemporary());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        return 0;
    }

    /**
     * @param targetCluster 目标集群
     * @param ses           同步实体
     * @return 1 表示目标集群存在该数据库 -1 表示目标集群不存在该数据库
     */
    public static int dbExists(String targetCluster, List<SyncEntity> ses) {
        Set<String> dbs = getDBS("target", ses);
        MyHiveMetaStoreClient dhmsc = null;
        for (String dbName : dbs) {
            try {
                //  获得目标集群的HMSC对象
                dhmsc = HMSCUtil.getHMSC(targetCluster + ".hive.conf");
                //  检查目标集群是否存在该数据库，如果不存在则会直接报异常NoSuchObjectException
                Database db = dhmsc.getDatabase(dbName);
                if (db != null) {
                    logger.error("目标集群" + targetCluster + "存在数据库" + dbName + "，不适合使用极速模式，请换成普通模式");
                    return 1;
                }
            } catch (NoSuchObjectException e) {
                logger.info(e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (dhmsc != null) {
                    dhmsc.close();
                }
            }
        }
        return -1;
    }

    /**
     * @param sourceCluster 源集群
     * @param ses           同步实体
     */
    public static void udfCheck(String sourceCluster, List<SyncEntity> ses) {
        Set<String> dbs = getDBS("source", ses);
        MyHiveMetaStoreClient shmsc = null;
        for (String dbName : dbs) {
            try {
                //  获得目标集群的HMSC对象
                shmsc = HMSCUtil.getHMSC(sourceCluster + ".hive.conf");
                List<String> functions = shmsc.getFunctions(dbName, "*");
                if (functions != null && functions.size() > 0) {
                    logger.info("源集群" + sourceCluster + "数据库" + dbName + "存在UDF" + functions + "，当前数据同步不会对UDF进行同步，请知悉");
                }
            } catch (TException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (shmsc != null) {
                    shmsc.close();
                }
            }
        }
    }

    public static HashSet<String> getDBS(String type, List<SyncEntity> ses) {
        HashSet<String> dbs = new HashSet<String>();
        String dbName = null;
        for (SyncEntity se : ses) {
            if ("source".equalsIgnoreCase(type)) {
                dbName = se.getSourceDB();
            } else if ("target".equalsIgnoreCase(type)) {
                dbName = se.getTargetDB();
            }
            if (!dbs.contains(dbName)) {
                dbs.add(dbName);
            }
        }
        return dbs;
    }

    /**
     * @param targetCluster 目标集群名称
     * @param ses           表集合
     */
    public static void validateTargetDB(String targetCluster, List<SyncEntity> ses) {
        Set<String> dbs = new HashSet<String>();
        for (SyncEntity se : ses) {
            //  每个数据库只检测一次
            if (!dbs.contains(se.getTargetDB())) {
                dbs.add(se.getTargetDB());
                validateTargetDB(targetCluster, se.getTargetDB());
            }
        }
    }

    /**
     * 检测目标集群数据库是否存在，如果不存在则创建
     *
     * @param targetCluster 目标集群名称
     * @param dbName        数据库名称
     */
    public static void validateTargetDB(String targetCluster, String dbName) {
        MyHiveMetaStoreClient dhmsc = null;
        try {
            //  获得目标集群的HMSC对象
            dhmsc = HMSCUtil.getHMSC(targetCluster + ".hive.conf");

            //  检查目标集群是否存在该数据库，如果不存在则会直接报异常NoSuchObjectException
            dhmsc.getDatabase(dbName);
        } catch (NoSuchObjectException e) {
            try {
                logger.info("目标集群" + targetCluster + "不存在数据库" + dbName);
                //  如果目标集群不存在该数据库则创建
                Database ddb = new Database();
                ddb.setName(dbName);
                dhmsc.createDatabase(ddb);
                logger.info("目标集群" + targetCluster + "数据库创建成功" + dbName);
            } catch (TException tException) {
                logger.error("目标集群" + targetCluster + "数据库创建失败" + dbName);
                tException.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dhmsc != null) {
                dhmsc.close();
            }
        }
    }

    public static List<String> getTables(String sourceCluster, String dbName, String tablePattern) {
        try {
            MyHiveMetaStoreClient hmsc = HMSCUtil.getHMSC(sourceCluster + ".hive.conf");

            List<String> tables = hmsc.getTables(dbName, tablePattern);

            List<String> items = new ArrayList<>();
            for (String table : tables) {
                //  获得源表对象
                Table sTable = hmsc.getTable(dbName, table);
                //  获得表类型
                String tableType = sTable.getTableType();
                if (tableType.equalsIgnoreCase("MANAGED_TABLE") || tableType.equalsIgnoreCase("EXTERNAL_TABLE") || tableType.equalsIgnoreCase("VIRTUAL_VIEW")) {
                    items.add(table);
                }
            }

            return items;
        } catch (MetaException metaException) {
            metaException.printStackTrace();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param args 命令行参数
     * @return 返回需要同步的实体集合
     */
    public static List<SyncEntity> getSyncEntities(String[] args) {
        ArrayList<SyncEntity> ses = new ArrayList<SyncEntity>();
        if (args == null || args.length < 3) {
            logger.error("参数个数有误，请重新输入");
            System.exit(1);
        } else {

            List<String> tables = new ArrayList<String>();
            //  源集群名称
            String sourceCluster = args[0];
            //  目标集群名称
            String targetCluster = args[1];
            //  源格式有可能是db.t1 db.t1,t2 db.*
            String source = args[2];
            String[] arrs = source.split("\\.");
            //  获得数据库
            String dbName = arrs[0];
            //  获得数据表，数据表可以是正则表达式
            String tablePattern = arrs[1];
            if (tablePattern.contains(",")) {
                //  表的表达式中包含逗号
                String[] ts = tablePattern.split(",");
                for (String t : ts) {
                    //  获得所有数据表
                    List<String> list = getTables(sourceCluster, dbName, t);
                    tables.addAll(list);
                }
            } else {
                //  获得所有数据表
                tables = getTables(sourceCluster, dbName, tablePattern);
            }
            //  封装SyncEntity
            for (String table : tables) {
                SyncEntity se = new SyncEntity(sourceCluster, dbName, table, targetCluster, dbName, table);
                ses.add(se);
            }
            if (args.length >= 4 && !"-conf".equalsIgnoreCase(args[3])) {
                //  目标格式
                String target = args[3];
                String[] targetarrs = target.split("\\.");
                //  获得数据库
                String targetDBName = targetarrs[0];
                String targetTablePattern = targetarrs[1];
                if (targetTablePattern.contains(",")) {
                    //  目前不支持C7 C6 ZXH.t1,t2  ZZL.t2,t
                    logger.error("输入参数有误，暂不支持在同一次数据同步进程中对多张表分别进行重命名操作，当前参数：" + StringUtils.join(args, " "));
                    System.exit(1);
                }
                if (targetTablePattern.equalsIgnoreCase("*")) {
                    //  数据库进行了重命名，表名保持一致
                    for (SyncEntity se : ses) {
                        se.setTargetDB(targetDBName);
                    }
                } else if (targetTablePattern.contains("*") && !("*").equalsIgnoreCase(targetTablePattern)) {
                    //  数据库和数据表都进行了重命名
                    String[] strings = targetTablePattern.split("\\*");
                    if (strings.length == 1) {
                        //  前缀
                        if (targetTablePattern.indexOf("*") == 0) {
                            for (SyncEntity se : ses) {
                                se.setTargetDB(targetDBName);
                                se.setTargetTB(se.getTargetTB() + strings[0]);
                            }
                        } else {
                            //  后缀
                            for (SyncEntity se : ses) {
                                se.setTargetDB(targetDBName);
                                se.setTargetTB(strings[0] + se.getTargetTB());
                            }
                        }
                    } else if (strings.length == 2) {
                        //  前后缀都有
                        for (SyncEntity se : ses) {
                            se.setTargetDB(targetDBName);
                            se.setTargetTB(strings[0] + se.getTargetTB() + strings[1]);
                        }
                    }
                } else {
                    //  数据表的表达式中不包含通配符，说明只是针对单表进行同步
                    if (ses.size() == 1) {
                        ses.get(0).setTargetDB(targetDBName);
                        ses.get(0).setTargetTB(targetTablePattern);
                    } else {
                        logger.error("输入参数有误，源表个数和目标表个数不一致，当前参数：" + StringUtils.join(args, " "));
                        System.exit(0);
                    }
                }
            }
            return ses;
        }
        return null;
    }

    public static List<SyncEntity> getSyncEntitiesV2(String[] args) {
        ArrayList<SyncEntity> ses = new ArrayList<SyncEntity>();
        if (args == null || args.length < 3) {
            logger.error("参数个数有误，请重新输入");
            System.exit(1);
        } else {
            List<String> tables = new ArrayList<String>();
            //  源集群名称
            String sourceCluster = args[0];
            //  目标集群名称
            String targetCluster = args[1];
            //  源格式有可能是db.t1 db.t1,t2 db.*
            String source = args[2];
            String[] arrs = source.split("\\.");
            //  获得数据库
            String dbName = arrs[0];
            //  获得数据表，数据表可以是正则表达式
            String tablePattern = arrs[1];
            if (tablePattern.contains(",")) {
                //  表的表达式中包含逗号
                String[] ts = tablePattern.split(",");
                for (String t : ts) {
                    //  获得所有数据表
                    List<String> list = getTables(sourceCluster, dbName, t);
                    tables.addAll(list);
                }
            } else {
                //  获得所有数据表
                tables = getTables(sourceCluster, dbName, tablePattern);
            }
            //  封装SyncEntity
            for (String table : tables) {
                SyncEntity se = new SyncEntity(sourceCluster, dbName, table, targetCluster, dbName, table);
                ses.add(se);
            }
            if (args.length >= 4 && !"-conf".equalsIgnoreCase(args[3]) && !Constant.SPEED_MODE.equalsIgnoreCase(args[3])) {
                //  目标格式
                String target = args[3];
                //  数据库进行了重命名，表名保持一致
                if (!target.contains(".")) {
                    //  目标库表格式不包含.
                    //  cdp7 yanlian zxh_sync_data.* zzl_data
                    for (SyncEntity se : ses) {
                        //  修改数据库名
                        se.setTargetDB(target);
                    }
                } else {
                    String[] targetarrs = target.split("\\.");
                    //  获得数据库
                    String targetDBName = targetarrs[0];
                    //  目标库表格式包含.
                    if (!target.contains("=")) {
                        //  目标库表格式不包含=说明是单表同步
                        //  获得表名
                        String targetTableName = targetarrs[1];
                        if (ses.size() == 1) {
                            ses.get(0).setTargetDB(targetDBName);
                            ses.get(0).setTargetTB(targetTableName);
                        }else if(ses.size() <1){
                            logger.error("输入参数有误，原表不存在，当前参数：" + StringUtils.join(args, " "));
                            return null;
                        } else {
                            logger.error("输入参数有误，暂不支持在同一次数据同步进程中对多张表分别进行重命名操作，当前参数：" + StringUtils.join(args, " "));
                            return null;
                        }
                    } else {
                        //  目标库表格式包含=说明是多表同步，可能是同库也可能是异库
                        //  获得表名表达式
                        String targetTablePattern = targetarrs[1];
                        if (targetTablePattern.contains(",")) {
                            //  说明前后缀都有prefix=pe,suffix=sf
                            String[] items = targetTablePattern.split(",");
                            if (items.length == 2) {
                                for (String item : items) {
                                    String[] strings = item.split("=");
                                    if (item.contains(Constant.PREFIX)) {
                                        //  前缀
                                        for (SyncEntity se : ses) {
                                            se.setTargetDB(targetDBName);
                                            se.setTargetTB(strings[1] + se.getTargetTB());
                                        }
                                    } else if (item.contains(Constant.SUFFIX)) {
                                        //  后缀
                                        for (SyncEntity se : ses) {
                                            se.setTargetDB(targetDBName);
                                            se.setTargetTB(se.getTargetTB() + strings[1]);
                                        }
                                    }
                                }
                            } else {
                                logger.error("传入参数有误");
                                return null;
                            }
                        } else {
                            //  只有前缀或者后缀prefix=pe或者suffix=sf
                            String[] strings = targetTablePattern.split("=");
                            if (Constant.PREFIX.equalsIgnoreCase(strings[0])) {
                                //  前缀
                                for (SyncEntity se : ses) {
                                    se.setTargetDB(targetDBName);
                                    se.setTargetTB(strings[1] + se.getTargetTB());
                                }
                            } else if (Constant.SUFFIX.equalsIgnoreCase(strings[0])) {
                                //  后缀
                                for (SyncEntity se : ses) {
                                    se.setTargetDB(targetDBName);
                                    se.setTargetTB(se.getTargetTB() + strings[1]);
                                }
                            } else {
                                logger.error("");
                                return null;
                            }
                        }
                    }
                }
            }
            return ses;
        }
        return null;
    }

    /**
     * 策略strategy可以简写成-s，目前场景分为以下几种
     * 单表 -s single 或者s
     * 多表 -s multi 或者m
     * <p>
     * 单表
     * cdp7 yanlian -s single db.tb
     * cdp7 yanlian -s single db.tb db1.tb1
     * 多表
     * cdp7 yanlian -s multi db.*
     * cdp7 yanlian -s multi db.* db1{prefix=pe_,suffix=_sf}
     *
     * @return 返回需要同步的实体集合
     */
    public static List<SyncEntity> getSyncEntitiesV3(String[] args) {
        ArrayList<SyncEntity> ses = new ArrayList<SyncEntity>();
        if (args == null || args.length < 3) {
            logger.error("参数个数有误，请重新输入");
            System.exit(1);
        } else {
            List<String> tables = new ArrayList<String>();

            //  同步策略
            //  -s single单表
            //  -s multi多表
            String syncStrategy = PropUtil.getProValue(Constant.SYNC_STRATEGY);

            //  源集群名称
            String sourceCluster = args[0];
            //  目标集群名称
            String targetCluster = args[1];
            //  源格式有可能是db.t1 db.t1,t2 db.*
            String source = args[4];
            String[] arrs = source.split("\\.");
            //  获得数据库
            String dbName = arrs[0];
            //  获得数据表，数据表可以是正则表达式
            String tablePattern = arrs[1];
            if (tablePattern.contains(",")) {
                //  表的表达式中包含逗号
                String[] ts = tablePattern.split(",");
                for (String t : ts) {
                    //  获得所有数据表
                    List<String> list = getTables(sourceCluster, dbName, t);
                    tables.addAll(list);
                }
            } else {
                //  如果数据表表达式明确声明为单表，但是同步模式为多表则返回null
                if (!tablePattern.contains("*") && Constant.SYNC_STRATEGY_MULTI.equalsIgnoreCase(syncStrategy)) {
                    logger.error("输入的同步策略有误-s single为单表同步，-s multi为多表同步，当前指定的同步策略为" + syncStrategy + "，声明的源表为" + source);
                    return null;
                }
                //  获得所有数据表
                tables = getTables(sourceCluster, dbName, tablePattern);
            }
            //  封装SyncEntity
            for (String table : tables) {
                SyncEntity se = new SyncEntity(sourceCluster, dbName, table, targetCluster, dbName, table);
                ses.add(se);
            }

            if (Constant.SYNC_STRATEGY_SINGLE.equalsIgnoreCase(syncStrategy) && ses.size() > 1) {
                logger.error("输入的同步策略有误-s single为单表同步，-s multi为多表同步，当前指定的同步策略为" + syncStrategy + "，解析出的源表为" + ses);
                return null;
            }

            if (args.length >= 6 && !args[5].contains("-")) {
                //  目标格式
                String target = args[5];

                if (!target.contains(".") && !target.contains("{")) {
                    //  目标库表格式不包含.
                    //  cdp7 yanlian zxh_sync_data.* zzl_data
                    for (SyncEntity se : ses) {
                        //  修改数据库名
                        se.setTargetDB(target);
                    }
                } else if (!target.contains(".") && target.contains("{") && target.contains("}")) {
                    //  替换}
                    target = target.replace("}", "");
                    //  数据库进行了重命名，表名进行了前缀或者后缀声明
                    String[] targetarrs = target.split("\\{");
                    //  获得数据库
                    String targetDBName = targetarrs[0];
                    //  获得表名表达式
                    String targetTablePattern = targetarrs[1];
                    if (!targetTablePattern.contains(",")) {
                        //  只有前缀或者后缀prefix=pe或者suffix=sf
                        String[] strings = targetTablePattern.split("=");
                        if (Constant.PREFIX.equalsIgnoreCase(strings[0])) {
                            //  前缀
                            for (SyncEntity se : ses) {
                                se.setTargetDB(targetDBName);
                                se.setTargetTB(strings[1] + se.getTargetTB());
                            }
                        } else if (Constant.SUFFIX.equalsIgnoreCase(strings[0])) {
                            //  后缀
                            for (SyncEntity se : ses) {
                                se.setTargetDB(targetDBName);
                                se.setTargetTB(se.getTargetTB() + strings[1]);
                            }
                        } else {
                            logger.error("");
                            return null;
                        }
                    } else {
                        //  说明前后缀都有prefix=pe,suffix=sf
                        String[] items = targetTablePattern.split(",");
                        if (items.length == 2) {
                            for (String item : items) {
                                String[] strings = item.split("=");
                                if (item.contains(Constant.PREFIX)) {
                                    //  前缀
                                    for (SyncEntity se : ses) {
                                        se.setTargetDB(targetDBName);
                                        se.setTargetTB(strings[1] + se.getTargetTB());
                                    }
                                } else if (item.contains(Constant.SUFFIX)) {
                                    //  后缀
                                    for (SyncEntity se : ses) {
                                        se.setTargetDB(targetDBName);
                                        se.setTargetTB(se.getTargetTB() + strings[1]);
                                    }
                                }
                            }
                        } else {
                            logger.error("传入参数有误");
                            return null;
                        }
                    }
                } else if (target.contains(".") && !target.contains("{") && !target.contains("}")) {
                    //  目标库表表达式包含.
                    String[] targetarrs = target.split("\\.");
                    //  获得数据库
                    String targetDBName = targetarrs[0];
                    //  获得表名
                    String targetTableName = targetarrs[1];
                    if (ses.size() == 1) {
                        ses.get(0).setTargetDB(targetDBName);
                        ses.get(0).setTargetTB(targetTableName);
                    } else if(ses.size() <1){
                        logger.error("输入参数有误，原表不存在，当前参数：" + StringUtils.join(args, " "));
                        return null;
                    }
                    else{
                        logger.error("输入参数有误，暂不支持在同一次数据同步进程中对多张表分别进行重命名操作，当前参数：" + StringUtils.join(args, " "));
                        return null;
                    }
                }
            }
            return ses;
        }
        return null;
    }

    public static Table replaceSerDe(Table table,String serializationLib){
        Table resultTable = table.deepCopy();
        StorageDescriptor sd = table.getSd().deepCopy();
        SerDeInfo serdeInfo = sd.getSerdeInfo().deepCopy();
        serdeInfo.setSerializationLib(serializationLib);
        sd.setSerdeInfo(serdeInfo);
        resultTable.setSd(sd);
        return resultTable;
    }

    public static void main(String[] args) {
        Authkrb5.authkrb5Default();
        OptionParserUtil.optionParse(args);
        List<SyncEntity> ses = getSyncEntitiesV3(args);
        System.out.println(ses);
    }
}
