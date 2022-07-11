package com.spdb.hive.sync.service;

import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.entity.MainTask;
import com.spdb.hive.sync.entity.SubTask;
import com.spdb.hive.sync.entity.SyncEntity;
import com.spdb.hive.sync.start.Start;
import com.spdb.hive.sync.util.hdfs.HDFSUtil;
import com.spdb.hive.sync.util.hive.HMSCUtil;
import com.spdb.hive.sync.util.hive.PartitionUtil;
import com.spdb.hive.sync.util.hive.StringUtils;
import com.spdb.hive.sync.util.hive.TableUtil;
import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MyHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * project：hive-sync
 * package：com.spdb.hive.replication.service
 * author：zhouxh
 * time：2021-03-30 10:03
 * description：HiveMetaStoreClient业务实现
 */
public class HMSCService {

    private SubTaskService subTaskService = new SubTaskService();

    private final static Logger logger = LogManager.getLogger(HMSCService.class);

    /**
     * @param mid  主任务对象
     * @param se   数据同步实体
     * @param mode 运行模式
     * @return 程序执行有异常返回-1 表元数据无差异返回0 表元数据有差异返回生成的子任务ID
     */
    public int syncMetadata(int mid, SyncEntity se, String mode) {
        //  获得源集群名称
        String sourceCluster = se.getSourceCluster();
        //  获得目标集群名称
        String targetCluster = se.getTargetCluster();
        //  源数据库
        String sourceDB = se.getSourceDB();
        //  源数据表
        String sourceTB = se.getSourceTB();
        //  目标数据库
        String targetDB = se.getTargetDB();
        //  目标数据表
        String targetTB = se.getTargetTB();
        logger.info("开始处理" + sourceDB + "." + sourceTB + "的元数据");

        MyHiveMetaStoreClient shmsc = null;
        MyHiveMetaStoreClient dhmsc = null;
        try {
            //  获得源集群的HMSC对象
            shmsc = HMSCUtil.getHMSC(sourceCluster + ".hive.conf");
            //  获得目标集群的HMSC对象
            dhmsc = HMSCUtil.getHMSC(targetCluster + ".hive.conf");
            //  获得源表对象
            Table sTable = shmsc.getTable(sourceDB, sourceTB);

            //  获得表类型
            String tableType = sTable.getTableType();


            //tod ，同步视图----------------------------------------------------------------------------------
            if (tableType.equalsIgnoreCase("VIRTUAL_VIEW")){
                if (shmsc.tableExists(sourceDB, sourceTB)) {
                    boolean isPartitionTable = isPartitionTable(sTable);
                    List<Partition> sPartitions = new ArrayList<Partition>();
                    //判断是否是急速模式
                    if (Constant.SPEED_MODE.equalsIgnoreCase(mode)) {
                        logger.info("当前元数据同步为极速模式运行");
                        //  极速模式
                        //  表不存在处理方式，分区和非分区都需要调用这个方法
                        int subTaskId = view4NotExists(mid, dhmsc, sTable, tableType, se);
                        if (subTaskId > 0) {
                            //  如果是分区表并且分区个数>0则继续处理分区
                                return subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_SUCCESS) > 0 ? subTaskId : -1;
                        }
                    }
                    else
                        //新任务模式
                    {
                        //  检查目标集群是否存在该表
                        boolean exists = dhmsc.tableExists(targetDB, targetTB);
                        if (!exists) {
                            //设置视图类型
                            tableType="VIRTUAL_VIEW";
                            // 表不存在处理方式，分区和非分区都需要调用这个方法
                            int subTaskId = view4NotExists(mid, dhmsc, sTable, tableType, se);
                            if (subTaskId > 0) {
                                return subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_SUCCESS) > 0 ? subTaskId : -1;
                            }
                        } else {
                            //如果目标表存在，以目标表类型为准
                            tableType=dhmsc.getTable(targetDB,targetTB).getTableType();

                            int subTaskId = view4Exists(mid, null, dhmsc, sTable, tableType, se);
                            if (subTaskId > 0) {
                                //  更新子任务对象
                                return subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_SUCCESS) > 0 ? subTaskId : -1;
                            } else if (subTaskId == 0) {
                                return 0;
                            }
                        }
                    }
                } else {
                    logger.error("源集群不存在该表" + sourceDB + "." + sourceTB);
                }
                return -1;
            }
            //tod,同步表----------------------------------------------------------------------------
            // 同步表，则判断是否是内外表
            else if (tableType.equalsIgnoreCase("MANAGED_TABLE") || tableType.equalsIgnoreCase("EXTERNAL_TABLE")) {
                boolean exists1 = dhmsc.tableExists(targetDB, targetTB);
                if (!exists1) {
                    //当目标集群无表时，修改tableType，6或者7默认建表类型不一致
                    //目标集群是6还是7，如果是6则tableType=“ MANAGED_TABLE”，如果是7则tableType="EXTERNAL_TABLE"
                    if (PropUtil.getProValue(targetCluster + ".version").contains("7")) {
                        StringUtils.printstrX_X("目标集群表不存在，设置表类型为EXTERNAL_TABLE");
                        tableType = "EXTERNAL_TABLE";
                    } else if (targetCluster.contains("6")) {
                        tableType = "MANAGED_TABLE";//MANAGED_TABLE
                        StringUtils.printstrX_X("目标集群表不存在，设置表类型为MANAGED_TABLE");
                    }
                } else {
                    tableType = dhmsc.getTable(targetDB, targetTB).getTableType();
                    StringUtils.printstrX_X("目标集群表存在，设置表类型为"+tableType);
//                tableType = "EXTERNAL_TABLE";
                }
                if (shmsc.tableExists(sourceDB, sourceTB)) {

                    //  判断源表是否为分区表
                    boolean isPartitionTable = isPartitionTable(sTable);

                    List<Partition> sPartitions = new ArrayList<Partition>();
                    if (isPartitionTable) {
                        //  如果表为分区表且分区个数超过32767则给出WARN提示并跳过该表
                        //  获得源表分区个数
                        sPartitions = shmsc.listPartitions(sourceDB, sourceTB, Short.MAX_VALUE);

                        if (sPartitions.size() >= Short.MAX_VALUE) {
                            logger.warn("源表分区数过大，跳过该表" + sourceDB + "." + sourceTB);
                            return -1;
                        }
                    }

                    if (!exists1) {
                        //当目标集群无表时，修改tableType，6或者7默认建表类型不一致
                        //目标集群是6还是7，如果是6则tableType=“ MANAGED_TABLE”，如果是7则tableType="EXTERNAL_TABLE"
                        if (PropUtil.getProValue(targetCluster + ".version").contains("7")) {
                            tableType = "EXTERNAL_TABLE";
                        } else if ( targetCluster.contains("6")) {
                            tableType = "MANAGED_TABLE";
                        }

                    } else{
                        tableType=dhmsc.getTable(targetDB,targetTB).getTableType();
                    }


                    if (Constant.SPEED_MODE.equalsIgnoreCase(mode)) {
                        logger.info("当前元数据同步为极速模式运行");
                        //  极速模式
                        //  表不存在处理方式，分区和非分区都需要调用这个方法
                        int subTaskId = processNonPartitionTable4NotExists(mid, dhmsc, sTable, tableType, se);
                        if (subTaskId > 0) {
                            //  如果是分区表并且分区个数>0则继续处理分区
                            if (isPartitionTable && sPartitions.size() > 0) {
                                StringUtils.printstrX_X("源表是分区表，且分区大于0");
                                //  分区表处理方式，除了需要改变表级别location值之外还需要修改每个分区中数据location值
                                boolean partitionFlag = processPartitionTable4NotExists(dhmsc, sPartitions, tableType, se);
                                if (partitionFlag) {
                                    //  更新子任务对象
                                    return subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_SUCCESS) > 0 ? subTaskId : -1;
                                } else {
                                    subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_FAIL);
                                    return -1;
                                }
                            } else {
                                //  更新子任务对象
                                return subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_SUCCESS) > 0 ? subTaskId : -1;
                            }
                        }
                    }
                    else
                        {
                        //  检查目标集群是否存在该表
                        boolean exists = dhmsc.tableExists(targetDB, targetTB);
                        if (!exists) {
                            StringUtils.printstrX_X("目标集群不存在需要同步的表"+sTable+":"+tableType);
                            //当目标集群无表时，修改tableType，6或者7默认建表类型不一致
                            //目标集群是6还是7，如果是6则tableType=“ MANAGED_TABLE”，如果是7则tableType="EXTERNAL_TABLE"
                            // 表不存在处理方式，分区和非分区都需要调用这个方法

                            int subTaskId = processNonPartitionTable4NotExists(mid, dhmsc, sTable, tableType, se);

                            if (subTaskId > 0) {
                                //  如果是分区表并且分区个数>0则继续处理分区
                                if (isPartitionTable && sPartitions.size() > 0) {
                                    //  分区表处理方式，除了需要改变表级别location值之外还需要修改每个分区中数据location值
                                    StringUtils.printstrX_X("源表是分区表，且分区大于0");
                                    boolean partitionFlag = processPartitionTable4NotExists(dhmsc, sPartitions, tableType, se);
                                    if (partitionFlag) {
                                        //  更新子任务对象
                                        return subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_SUCCESS) > 0 ? subTaskId : -1;
                                    } else {
                                        subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_FAIL);
                                        return -1;
                                    }
                                } else {
                                    //  更新子任务对象
                                    return subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_SUCCESS) > 0 ? subTaskId : -1;
                                }
                            }
                        } else {
                            StringUtils.printstrX_X("目标集群存在需要同步的表");
                            //如果目标表存在，以目标表类型为准
                            //tableType=dhmsc.getTable(targetDB,targetTB).getTableType();

                            //  非分区表存在处理方式
                            int subTaskId = -1;
                            if (!isPartitionTable) {
                                StringUtils.printstrX_X("源表是非分区表，替换表级路径");
                                subTaskId = processNonPartitionTable4Exists(mid, null, dhmsc, sTable, tableType, se);
                            } else {
                                //  分区表处理方式，除了需要改变表级别location值之外还需要修改每个分区中数据location值
                                StringUtils.printstrX_X("源表是分区表，替换分区级路径");
                                subTaskId = processPartitionTable4Exists(mid, dhmsc, sTable, sPartitions, tableType, se);
                            }

                            if (subTaskId > 0) {
                                //  更新子任务对象
                                return subTaskService.updateSubTaskMetadataSyncStatus(subTaskId, Constant.TASK_STATUS_SUCCESS) > 0 ? subTaskId : -1;
                            } else if (subTaskId == 0) {
                                return 0;
                            }
                        }
                    }
                } else {
                    logger.error("源集群不存在该表" + sourceDB + "." + sourceTB);
                }
            }else{
                logger.error("命令输入有误，请检查");
                return -1;
            }

        } catch (TException e) {
            e.printStackTrace();
            return -1;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        } finally {
            HMSCUtil.close(shmsc);
            HMSCUtil.close(dhmsc);
        }
        return -1;
    }
    /**
     * 非分区表不存在处理逻辑
     * 1、获得源表对象；
     * 2、根据源表deepCopy出新表，修改新表的location，在目标集群create新表；
     *
     * @param mid    主任务id
     * @param dhmsc  目标集群HiveMetaStoreClient
     * @param sTable 源数据表
     */
    public int processNonPartitionTable4NotExists(int mid, MyHiveMetaStoreClient dhmsc, Table sTable, String tableType, SyncEntity se) {
        try {
            //  源集群名称
            String sourceCluster = se.getSourceCluster();
            //  目标集群名称
            String targetCluster = se.getTargetCluster();
            //  源数据库
            String sourceDB = se.getSourceDB();
            //  源数据表
            String sourceTB = se.getSourceTB();
            //  目标数据库
            String targetDB = se.getTargetDB();
            //  目标数据表
            String targetTB = se.getTargetTB();

            //  目标集群不存在该表
            logger.info("目标集群" + targetCluster + "不存在数据表" + sourceDB + "." + sourceTB + "[" + targetDB + "." + targetTB + "]");

            //  修改表级别的location
            Table dTable = replaceTableLocation(se, sTable, tableType);
            //           dTable.setTableType();

                dTable.setTableType(tableType);
//                if (PropUtil.getProValue(targetCluster + ".version").contains("6")) {
//                    if (tableType.equalsIgnoreCase("EXTERNAL_TABLE") && sTable.getTableType().equalsIgnoreCase("MANAGED_TABLE")) {
//                        dTable.putToParameters("transacional", "FALSE");
//                        dTable.putToParameters("EXTERNAL", "TRUE");
//                        StringUtils.printstrX_X("->6,内表到外表，设置  transacional=false  external=true");
//                    } else if (tableType.equalsIgnoreCase("MANAGED_TABLE") && sTable.getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
//                        dTable.putToParameters("transacional", "TRUE");
////                    dTable.putToParameters("TRANSLATED_TO_EXTERNAL","FALSE");
//                        dTable.putToParameters("EXTERNAL","FALSE");
////                    dTable.putToParameters("external.table.purge","FALSE");
//                        StringUtils.printstrX_X("->6,外表到内表，设置  transacional=true  external=false");
//                    }
//                }



            if (PropUtil.getProValue(sourceCluster + ".version").contains("6") && PropUtil.getProValue(targetCluster + ".version").contains("7")) {
               StringUtils.printstrX_X("6_>7:================—————————————————————————————————目标类型"+tableType);
                if(sTable.getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                    dTable = TableUtil.replaceSerDe(dTable, "org.apache.hadoop.hive.serde2.MultiDelimitSerDe");
                }
               if (tableType.equalsIgnoreCase("EXTERNAL_TABLE") && sTable.getTableType().equalsIgnoreCase("MANAGED_TABLE")) {
                    dTable.putToParameters("transacional", "FALSE");
                    dTable.putToParameters("EXTERNAL", "TRUE");
                } else if (tableType.equalsIgnoreCase("MANAGED_TABLE") && sTable.getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
                    dTable.putToParameters("transacional", "TRUE");
//                        newDTable.putToParameters("EXTERNAL","TRUE");
                }
            }else if(PropUtil.getProValue(sourceCluster + ".version").contains("7") && PropUtil.getProValue(targetCluster + ".version").contains("6")) {
                if(sTable.getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                    dTable = TableUtil.replaceSerDe(dTable, "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe");
                }
                StringUtils.printstrX_X("7_>6:================—————————————————————————————————目标类型"+tableType);
                if (tableType.equalsIgnoreCase("EXTERNAL_TABLE") && sTable.getTableType().equalsIgnoreCase("MANAGED_TABLE")) {
                    dTable.putToParameters("transacional", "FALSE");
                    dTable.putToParameters("EXTERNAL", "TRUE");
                } else if (tableType.equalsIgnoreCase("MANAGED_TABLE") && sTable.getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
                    dTable.putToParameters("transacional", "TRUE");
                    dTable.putToParameters("EXTERNAL","FALSE");
                }
            }

//

            //  创建子任务对象并存储数据库
            SubTask subTask = createAndStoreNewSubTask(sourceCluster, sourceDB, sourceTB, targetCluster, targetDB, targetTB, sTable.getSd().getLocation(), dTable.getSd().getLocation(), mid);

            if (subTask != null && subTask.getId() > 0) {
                try {
                    //  在目标集群创建目标表
                    dhmsc.createTable(dTable);
                    logger.info("目标集群" + targetCluster + "表级元数据同步成功" + sourceDB + "." + sourceTB + "[" + targetDB + "." + targetTB + "]");
                    return subTask.getId();
                } catch (TException e) {
                    e.printStackTrace();
                    subTaskService.updateSubTaskMetadataSyncStatus(subTask.getId(), Constant.TASK_STATUS_FAIL);
                    return -1;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }
    /**
     * 非分区视图不存在处理逻辑
     * 1、获得源表对象；
     * 2、根据源表deepCopy出新视图，在目标集群create新表；
     *
     * @param mid    主任务id
     * @param dhmsc  目标集群HiveMetaStoreClient
     * @param sTable 源数据视图
     */
    public int view4NotExists(int mid, MyHiveMetaStoreClient dhmsc, Table sTable, String tableType, SyncEntity se) {
        try {
            //  源集群名称
            String sourceCluster = se.getSourceCluster();
            //  目标集群名称
            String targetCluster = se.getTargetCluster();
            //  源数据库
            String sourceDB = se.getSourceDB();
            //  源数据表
            String sourceTB = se.getSourceTB();
            //  目标数据库
            String targetDB = se.getTargetDB();
            //  目标数据表
            String targetTB = se.getTargetTB();

            //  目标集群不存在该表
            logger.info("目标集群" + targetCluster + "不存在数据表" + sourceDB + "." + sourceTB + "[" + targetDB + "." + targetTB + "]");

            //  修改表级别的location
//            Table dTable = replaceTableLocation(se, sTable, tableType);

//            MyHiveMetaStoreClient shmsc = HMSCUtil.getHMSC("CDH6_BDS_UAT" + ".hive.conf");
//            Table parents = shmsc.getTable("test01", "student2");

            Table dTable = sTable.deepCopy();
            int timeNow=(int)(System.currentTimeMillis()/1000);
//            Table dTable = new Table();
            dTable.setDbName(targetDB);
            dTable.setTableName(targetTB);
            dTable.setOwner(sTable.getOwner());
            dTable.setCreateTime(timeNow);
            dTable.setLastAccessTime(timeNow);
            dTable.setRetention(timeNow);
            dTable.setTableType(TableType.VIRTUAL_VIEW.name());

            StorageDescriptor sd = new StorageDescriptor(sTable.getSd());
            sd.setLocation(null);
            dTable.setSd(sd);

            //  创建子任务对象并存储数据库
            SubTask subTask = createAndStoreNewSubTask(sourceCluster, sourceDB, sourceTB, targetCluster, targetDB, targetTB, sTable.getSd().getLocation(), dTable.getSd().getLocation(), mid);

            if (subTask != null && subTask.getId() > 0) {
                try {

                    //  在目标集群创建目标表
                    dhmsc.createTable(dTable);
                    logger.info("目标集群" + targetCluster + "表级元数据同步成功" + sourceDB + "." + sourceTB + "[" + targetDB + "." + targetTB + "]");
                    return subTask.getId();
                } catch (TException e) {
                    e.printStackTrace();
                    subTaskService.updateSubTaskMetadataSyncStatus(subTask.getId(), Constant.TASK_STATUS_FAIL);
                    return -1;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * 非分区表存在处理逻辑
     * 1、分别获得源表和目标表对象；
     * 2、调用table对象的compareTo比较源表和目标表
     * 2.1、重写了表级别的compareTo（不进行createTime、ownerType的比较）；
     * 2.2、重写了StorageDescriptor的compareTo（不进行表级location比较）；
     * 2.3、重写了TBaseHelper的compareTo(Map a, Map b)，用来处理表级别parameter比较，去除statistics信息；
     * 3、比较结果一致则说明表元数据无变化，无法同步；
     * 4、比较结果不一致，则直接从源表deepCopy出新表，修改新表的location，在目标集群alter新表；
     *
     * @param mid    主任务id
     * @param dhmsc  目标集群HiveMetaStoreClient
     * @param sTable 源数据表
     */
    public int processNonPartitionTable4Exists(int mid, SubTask subTask, MyHiveMetaStoreClient dhmsc, Table sTable, String tableType, SyncEntity se) {
        try {
            //  源集群名称
            String sourceCluster = se.getSourceCluster();
            //  目标集群名称
            String targetCluster = se.getTargetCluster();
            //  源数据库
            String sourceDB = se.getSourceDB();
            //  源数据表
            String sourceTB = se.getSourceTB();
            //  目标数据库
            String targetDB = se.getTargetDB();
            //  目标数据表
            String targetTB = se.getTargetTB();

            logger.info("目标集群" + targetCluster + "存在数据表" + targetDB + "." + targetTB + "，开始比对元数据");

            //  在目标集群获得目标数据表
            Table dTable = dhmsc.getTable(targetDB, targetTB);

            //  对源集群和目标集群的数据表进行元数据比对,这里使用去除字段分割的属性对象来比较
            int comparison = 0;
            if (sTable.getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                Table withoutSerSouTable = TableUtil.replaceSerDe(sTable,"");
                Table withoutSerDesTable = TableUtil.replaceSerDe(dTable,"");
                 comparison = TableUtil.tableCompare( withoutSerSouTable , withoutSerDesTable);
            }else {
                 comparison = TableUtil.tableCompare( sTable , dTable);
            }
            //  比对结果不为0说明源的结构发生了变化
            if (comparison != 0) {

                //  表级别元数据发生了变化，这时候不能简单直接的创建子任务对象，因为之前步骤中可能因为分区级别元数据变化已经创建了子任务对象
                if (subTask == null) {
                    subTask = createAndStoreNewSubTask(sourceCluster, sourceDB, sourceTB, targetCluster, targetDB, targetTB, sTable.getSd().getLocation(), dTable.getSd().getLocation(), mid);
                }

                try {
                    logger.info("目标集群" + targetCluster + "和源集群表级元数据不一致，开始同步" + sourceDB + "." + sourceTB);
                    //  根据源表拷贝出新表
                    Table newDTable = replaceTableLocation(se, sTable, tableType);
                    newDTable.setTableType(tableType);
//                    String sourceVersion = PropUtil.getProValue(sourceCluster + ".version");
//                    String targetVersion = PropUtil.getProValue(targetCluster + ".version");
                    if (PropUtil.getProValue(sourceCluster + ".version").contains("6") && PropUtil.getProValue(targetCluster + ".version").contains("7")) {
                        if (sTable.getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                            newDTable = TableUtil.replaceSerDe(newDTable, "org.apache.hadoop.hive.serde2.MultiDelimitSerDe");
                        }
                        if (tableType.equalsIgnoreCase("EXTERNAL_TABLE") && sTable.getTableType().equalsIgnoreCase("MANAGED_TABLE")) {
                            newDTable.putToParameters("transacional", "FALSE");
                            newDTable.putToParameters("EXTERNAL", "TRUE");
                        } else if (tableType.equalsIgnoreCase("MANAGED_TABLE") && sTable.getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
                            newDTable.putToParameters("transacional", "TRUE");
//                        newDTable.putToParameters("EXTERNAL","TRUE");
                        }
                    }else if(PropUtil.getProValue(sourceCluster + ".version").contains("7") && PropUtil.getProValue(targetCluster + ".version").contains("6")) {
                        if (sTable.getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                            newDTable = TableUtil.replaceSerDe(newDTable, "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe");
                        }
                        if (tableType.equalsIgnoreCase("EXTERNAL_TABLE") && sTable.getTableType().equalsIgnoreCase("MANAGED_TABLE")) {
                            newDTable.putToParameters("transacional", "FALSE");
                            newDTable.putToParameters("EXTERNAL", "TRUE");
                        } else if (tableType.equalsIgnoreCase("MANAGED_TABLE") && sTable.getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
                            newDTable.putToParameters("transacional", "TRUE");
                            newDTable.putToParameters("EXTERNAL","FALSE");
                        }
                    }

                    //  修改表
                    dhmsc.alter_table(targetDB, targetTB, newDTable);
                    logger.info("目标集群" + targetCluster + "表级元数据同步成功" + sourceDB + "." + sourceTB);
                    return subTask.getId();
                } catch (TException e) {
                    e.printStackTrace();
                    subTaskService.updateSubTaskMetadataSyncStatus(subTask.getId(), Constant.TASK_STATUS_FAIL);
                    return -1;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                logger.info("源集群" + sourceDB + "." + sourceTB + "表级元数据未发生变化，无需同步");
                if (subTask == null) {
                    //  代码走到这里说明在这次元数据比对过程中表级元数据和分区级元数据都未发生变化
                    return 0;
                } else {
                    return subTask.getId();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }
    /**
     * 非分区表存在处理逻辑
     * 1、分别获得源表和目标表对象；
     * 2、调用table对象的compareTo比较源表和目标表
     * 2.1、重写了表级别的compareTo（不进行createTime、ownerType的比较）；
     * 2.2、重写了StorageDescriptor的compareTo（不进行表级location比较）；
     * 2.3、重写了TBaseHelper的compareTo(Map a, Map b)，用来处理表级别parameter比较，去除statistics信息；
     * 3、比较结果一致则说明表元数据无变化，无法同步；
     * 4、比较结果不一致，则直接从源表deepCopy出新表，修改新表的location，在目标集群alter新表；
     *
     * @param mid    主任务id
     * @param dhmsc  目标集群HiveMetaStoreClient
     * @param sTable 源数据表
     */
    public int view4Exists(int mid, SubTask subTask, MyHiveMetaStoreClient dhmsc, Table sTable, String tableType, SyncEntity se) {
        try {
            //  源集群名称
            String sourceCluster = se.getSourceCluster();
            //  目标集群名称
            String targetCluster = se.getTargetCluster();
            //  源数据库
            String sourceDB = se.getSourceDB();
            //  源数据表
            String sourceTB = se.getSourceTB();
            //  目标数据库
            String targetDB = se.getTargetDB();
            //  目标数据表
            String targetTB = se.getTargetTB();

            logger.info("目标集群" + targetCluster + "存在数据表" + targetDB + "." + targetTB + "，开始比对元数据");

            //  在目标集群获得目标数据表
            Table dTable = dhmsc.getTable(targetDB, targetTB);

            //  对源集群和目标集群的数据表进行元数据比对
            int comparison = TableUtil.tableCompare(sTable, dTable);
            //  比对结果不为0说明源的结构发生了变化
            if (comparison != 0) {

                //  表级别元数据发生了变化，这时候不能简单直接的创建子任务对象，因为之前步骤中可能因为分区级别元数据变化已经创建了子任务对象
                if (subTask == null) {
                    subTask = createAndStoreNewSubTask(sourceCluster, sourceDB, sourceTB, targetCluster, targetDB, targetTB, sTable.getSd().getLocation(), dTable.getSd().getLocation(), mid);
                }

                try {
                    logger.info("目标集群" + targetCluster + "和源集群表级元数据不一致，开始同步" + sourceDB + "." + sourceTB);
                    //  根据源表拷贝出新表
//                    Tble newDTable = replaceTableLocation(se, sTable, tableType);
                    Table newDTable = sTable.deepCopy();
                    int timeNow=(int)(System.currentTimeMillis()/1000);
//            Table dTable = new Table();
                    newDTable.setDbName(targetDB);
                    newDTable.setTableName(targetTB);
                    newDTable.setOwner(sTable.getOwner());
                    newDTable.setCreateTime(timeNow);
                    newDTable.setLastAccessTime(timeNow);
                    newDTable.setRetention(timeNow);
                    newDTable.setTableType(TableType.VIRTUAL_VIEW.name());

                    StorageDescriptor sd = new StorageDescriptor(sTable.getSd());
                    sd.setLocation(null);
                    newDTable.setSd(sd);

                    //  todo修改表--------------------------------------------------------------------------------
                    dhmsc.alter_table(targetDB, targetTB, newDTable);
                    logger.info("目标集群" + targetCluster + "表级元数据同步成功" + sourceDB + "." + sourceTB);
                    return subTask.getId();
                } catch (TException e) {
                    e.printStackTrace();
                    subTaskService.updateSubTaskMetadataSyncStatus(subTask.getId(), Constant.TASK_STATUS_FAIL);
                    return -1;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                logger.info("源集群" + sourceDB + "." + sourceTB + "表级元数据未发生变化，无需同步");
                if (subTask == null) {
                    //  代码走到这里说明在这次元数据比对过程中表级元数据和分区级元数据都未发生变化
                    return 0;
                } else {
                    return subTask.getId();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * 分区表不存在处理逻辑
     * 1、获得源表对象；
     * 2、获得源表所有分区（最多32767个），如果分区数达到这个数量级直接给出WARN提示并跳过该表；
     * 3、根据源表deepCopy出新表，修改新表的location，在目标集群create新表；
     * 4、如果分区个数不超过32767，根据源表deepCopy出新表，修改新表的location，在目标集群create新表，
     * 遍历源表所有分区，修改每个分区的location，deepCopy出新分区，修改新分区的location，加入集合，最后一次性在目标表添加所有分区；
     *
     * @param dhmsc       目标集群HiveMetaStoreClient
     * @param sPartitions 源表分区集合
     * @param tableType   表类型
     * @param se          同步实体
     * @return
     */
    public boolean processPartitionTable4NotExists(MyHiveMetaStoreClient dhmsc, List<Partition> sPartitions, String tableType, SyncEntity se) {
        try {
            //  源集群名称
            String sourceCluster = se.getSourceCluster();
            //  目标集群名称
            String targetCluster = se.getTargetCluster();
            //  源数据库
            String sourceDB = se.getSourceDB();
            //  源数据表
            String sourceTB = se.getSourceTB();

            // 判断目标集群的字段分隔符类名
            String realPartitionSerdInfo = null;
            if (sPartitions.get(0).getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                if (PropUtil.getProValue(sourceCluster + ".version").contains("6") && PropUtil.getProValue(targetCluster + ".version").contains("7")) {
                    realPartitionSerdInfo = "org.apache.hadoop.hive.serde2.MultiDelimitSerDe";
                }else if(PropUtil.getProValue(sourceCluster + ".version").contains("7") && PropUtil.getProValue(targetCluster + ".version").contains("6")){
                    realPartitionSerdInfo = "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe";
                }
            }
                logger.info("该表为分区表，开始处理分区，" + sourceDB + "." + sourceTB + "，该表分区个数：" + sPartitions.size());
            ArrayList<Partition> dPartitions = new ArrayList<>();
            for (Partition sPartition : sPartitions) {
                //  修改分区数据存储location值
                Partition dPartition = replacePartitionLocation(se, sPartition, tableType);
                // 修正分区字段分隔符类名
                if (sPartition.getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                    dPartition = replacePartitionSerdInfo(dPartition, realPartitionSerdInfo);
                }
                dPartitions.add(dPartition);
            }

            //  将待添加分区集合一次性添加
            if (dPartitions.size() > 0) {
                logger.info("目标集群" + targetCluster + "的" + sourceDB + "." + sourceTB + "需要添加的分区个数为" + dPartitions.size());
                dhmsc.add_partitions(dPartitions);
            }
            logger.info("该表为分区表，分区处理成功" + sourceDB + "." + sourceTB);

            return true;
        } catch (TException e) {
            e.printStackTrace();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 1、获得源表对象；
     * 2、获得源表所有分区（最多32767个），如果分区数达到这个数量级直接给出WARN提示并跳过该表；
     * 3、获得目标表对象和目标表所有分区；
     * 4、比较源表分区和目标表分区，遍历源分区，如果源分区中存在而目标分区中不存在，则deepCopy出新分区，修改新分区location，
     * 加入集合最后一次性在目标表添加所有分区，如果源和目标都存在，则比较Partition的compareTo方法，
     * 比较分区是否有变化，如果有变化则alter目标分区；
     * 5、上面主要是对分区级别元数据的比较和同步，表级别的元数据比较和同步方式与无分区表存在的方式一致；
     *
     * @param mid         主任务id
     * @param dhmsc       目标集群HiveMetaStoreClient
     * @param sTable      源表对象
     * @param sPartitions 源表分区集合
     */
    public int processPartitionTable4Exists(int mid, MyHiveMetaStoreClient dhmsc, Table sTable, List<Partition> sPartitions, String tableType, SyncEntity se) {
        try {
            //  源集群名称
            String sourceCluster = se.getSourceCluster();
            //  目标集群名称
            String targetCluster = se.getTargetCluster();
            //  源数据库
            String sourceDB = se.getSourceDB();
            //  源数据表
            String sourceTB = se.getSourceTB();
            //  目标数据库
            String targetDB = se.getTargetDB();
            //  目标数据表
            String targetTB = se.getTargetTB();

            logger.info("该表为分区表，开始处理分区，" + sourceDB + "." + sourceTB + "该表分区个数：" + sPartitions.size());

            String realPartitionSerdInfo = null;
            if (sPartitions.get(0).getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                if (PropUtil.getProValue(sourceCluster + ".version").contains("6") && PropUtil.getProValue(targetCluster + ".version").contains("7")) {
                    realPartitionSerdInfo = "org.apache.hadoop.hive.serde2.MultiDelimitSerDe";
                }
                if(PropUtil.getProValue(sourceCluster + ".version").contains("7") && PropUtil.getProValue(targetCluster + ".version").contains("6")){
                    realPartitionSerdInfo = "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe";
                }
            }

            //  获得目标表分区个数
            List<Partition> dPartitions = dhmsc.listPartitions(targetDB, targetTB, Short.MAX_VALUE);

            //  将源分区和目标分区进行转换封装Map<partition.getValues().toString(),partition>，便于进行源和目标的比较
            HashMap<String, Partition> sMapPartitions = transforPartitions(sPartitions);
            HashMap<String, Partition> dMapPartitions = transforPartitions(dPartitions);

            //  获得源分区和目标分区所有分区值集合
            Set<String> sPartitionKeys = sMapPartitions.keySet();
            Set<String> dPartitionKeys = dMapPartitions.keySet();

            //  存放需要新增的分区
            List<Partition> needAddPartitions = new ArrayList<Partition>();

            //  存放需要修改的分区
            List<Partition> needAlterPartitions = new ArrayList<Partition>();

            //  遍历源分区
            for (String sKey : sPartitionKeys) {

                //  判断目标分区是否存在该分区
                if (dPartitionKeys.contains(sKey)) {
                    //  目标分区存在该分区，继续比较partition的compareTo
                    // 去除分区字段分隔符属性再对比
                    int i = 0;
                    if (sMapPartitions.get(sKey).getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                        Partition withoutSerdeInfoSouPartition = replacePartitionSerdInfo(sMapPartitions.get(sKey),"");
                        Partition withoutSerdeInfoDesPartition = replacePartitionSerdInfo(dMapPartitions.get(sKey),"");
                        i = PartitionUtil.compareTo(withoutSerdeInfoSouPartition, withoutSerdeInfoDesPartition);
                    }else {
                        i = PartitionUtil.compareTo(sMapPartitions.get(sKey), dMapPartitions.get(sKey));
                    }

                    if (i != 0) {
                        //  分区发生变化需要修改
                        Partition newPartition = replacePartitionLocation(se, sMapPartitions.get(sKey), tableType);
                        //校正字段分隔符属性
                        if (sMapPartitions.get(sKey).getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                            newPartition = replacePartitionSerdInfo(newPartition,realPartitionSerdInfo);
                        }
                        needAlterPartitions.add(newPartition);
                        logger.info("源集群" + sourceDB + "." + sourceTB + "." + sKey + "分区级元数据发生变化，即将同步");
                    } else {
                        logger.info("源集群" + sourceDB + "." + sourceTB + "." + sKey + "分区级元数据未发生变化，无需同步");
                    }
                } else {
                    //  目标分区不存在该分区，根据源分区创建新分区
                    Partition newPartition = replacePartitionLocation(se, sMapPartitions.get(sKey), tableType);
                    if (sMapPartitions.get(sKey).getSd().getSerdeInfo().getSerializationLib().contains("MultiDelimitSerDe")){
                        newPartition = replacePartitionSerdInfo(newPartition,realPartitionSerdInfo);
                    }
                    //  将新分区加入到待添加分区集合
                    needAddPartitions.add(newPartition);
                    logger.info("目标集群" + targetCluster + "的" + targetDB + "." + targetTB + "不存在该分区" + sKey + "，即将添加");
                }
            }

            SubTask subTask = null;

            //  对于分区表存在的情况下，如果需要添加的分区个数或者需要修改的分区个数大于0，先创建子任务对象并存储
            if (needAddPartitions.size() > 0 || needAlterPartitions.size() > 0) {
                Table dTable = dhmsc.getTable(targetDB, targetTB);
                subTask = createAndStoreNewSubTask(sourceCluster, sourceDB, sourceTB, targetCluster, targetDB, targetTB, sTable.getSd().getLocation(), dTable.getSd().getLocation(), mid);
            }

            //  将待添加分区集合一次性添加
            if (needAddPartitions.size() > 0) {
                logger.info("目标集群" + targetCluster + "的" + sourceDB + "." + sourceTB + "需要添加的分区个数为" + needAddPartitions.size());
                dhmsc.add_partitions(needAddPartitions);
            }

            //  将待修改分区一次性修改
            if (needAlterPartitions.size() > 0) {
                logger.info("目标集群" + targetCluster + "的" + sourceDB + "." + sourceTB + "需要修改的分区个数为" + needAlterPartitions.size());
                dhmsc.alter_partitions(targetDB, targetTB, needAlterPartitions);
            }

            logger.info("该表为分区表，分区处理成功" + sourceDB + "." + sourceTB);

            //  上面的逻辑中如果分区表的分区有变化，先新增了子任务对象，元数据同步状态是wait，紧跟着对分区进行同步，
            //  同步成功后先不更新元数据同步状态为success，因为还要继续对表级别元数据进行比较，只有两者都成功，
            //  才将metadataSyncStatus更新为success
            //  表级别的元数据比较和同步方式与无分区表存在的方式一致
            return processNonPartitionTable4Exists(mid, subTask, dhmsc, sTable, tableType, se);

        } catch (TException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return -1;
    }



    /**
     * 判断表是否为分区表
     *
     * @param table 需要判断的表
     * @return true 分区表 false 非分区表
     */
    public boolean isPartitionTable(Table table) {
        //  获得表的分区字段key
        List<FieldSchema> partitionKeys = table.getPartitionKeys();

        if (partitionKeys != null && partitionKeys.size() > 0) {
            return true;
        }
        return false;
    }

    /**
     * @param se        同步实体
     * @param sTable    源表对象
     * @param tableType 表类型
     * @return 创建新表并替换表location值
     */
    public Table replaceTableLocation(SyncEntity se, Table sTable, String tableType) {
        //  根据源表拷贝出新表
        Table dTable = sTable.deepCopy();
        //  重命名库表名称
        dTable.setDbName(se.getTargetDB());
        dTable.setTableName(se.getTargetTB());
        //  获得源表数据位置信息
        String tableLocation = dTable.getSd().getLocation();
        //  替换表location值
        tableLocation = replaceLocation(se, tableLocation, tableType);

        dTable.getSd().setLocation(tableLocation);
        //  返回目标表
        return dTable;
    }

    /**
     * 创建新分区并替换分区location值
     *
     * @param se         同步实体
     * @param sPartition 源分区对象
     * @return 目标分区对象
     */
    public Partition replacePartitionLocation(SyncEntity se, Partition sPartition, String tableType) {
        //  根据源分区拷贝出新分区
        Partition dPartition = sPartition.deepCopy();
        //  重命名库表名称
        dPartition.setDbName(se.getTargetDB());
        dPartition.setTableName(se.getTargetTB());
        //  获得源分区的数据位置信息
        String partitionLocation = dPartition.getSd().getLocation();
        //  替换分区location值
        partitionLocation = replaceLocation(se, partitionLocation, tableType);
        dPartition.getSd().setLocation(partitionLocation);
        //  返回目标分区对象
        return dPartition;
    }

    /**
     * 校正分区对象serdeInfo属性
     * @param dPartition 目标分区对象
     * @param serdeInfoString 新属性值
     * @return 目标分区新对象
     */
    public Partition replacePartitionSerdInfo(Partition dPartition, String serdeInfoString) {
        Partition resultPartition = dPartition.deepCopy();
        StorageDescriptor sd = dPartition.getSd().deepCopy();
       SerDeInfo serdeInfo = sd.getSerdeInfo().deepCopy();
        serdeInfo.setSerializationLib(serdeInfoString);
        sd.setSerdeInfo(serdeInfo);
        resultPartition.setSd(sd);
        return resultPartition;
    }

    /**
     * 替换位置信息
     *
     * @param se        同步实体
     * @param sLocation 源位置信息
     * @param tableType 表类型
     * @return
     */
    public String replaceLocation(SyncEntity se, String sLocation, String tableType) {
        try {
            //  源集群名称
            String sourceCluster = se.getSourceCluster();
            //  目标集群名称
            String targetCluster = se.getTargetCluster();
            //  源数据库
            String sourceDB = se.getSourceDB();
            //  源数据表
            String sourceTB = se.getSourceTB();
            //  目标数据库
            String targetDB = se.getTargetDB();
            //  目标数据表
            String targetTB = se.getTargetTB();

            //  首先替换nameservice
            String d = sLocation.replace(PropUtil.getProValue(sourceCluster + ".hdfs.fs"), PropUtil.getProValue(targetCluster + ".hdfs.fs"));
            //  源集群版本
            String sourceVersion = PropUtil.getProValue(sourceCluster + ".version");
            //  目标集群版本
            String targetVersion = PropUtil.getProValue(targetCluster + ".version");

            if (!"cdp7".equalsIgnoreCase(sourceVersion) && "cdp7".equalsIgnoreCase(targetVersion)) {
                //  非cdp7同步到cdp7集群
                //  针对hive3 内部表存储路径为/warehouse/tablespace/managed/hive/db/table
                if (tableType.equalsIgnoreCase("MANAGED_TABLE")) {
//                    d = d.replace("user/hive/warehouse", "warehouse/tablespace/managed/hive");
                    //  库表重命名后对应的库表location也需要进行替换
                    d = d.replace(sourceDB, targetDB).replace(sourceTB, targetTB);
                    StringUtils.printstrX_X("同步到cdp7的内表，则目标表路径为：warehouse/tablespace/managed/hive");
                } else if (tableType.equalsIgnoreCase("EXTERNAL_TABLE")) {
                    //  针对hive3外部表默认存储路径为/warehouse/tablespace/external/hive/db/table，可以手动指定location
                    //  CDH6默认hive外部表存储路径为/user/hive/warehouse/db/table，可以手动指定location
                    //  这里的处理逻辑是如果CDH6的的外部表location使用的是默认路径，则到CDP7后放到/warehouse/tablespace/external/hive/db/table
                    //  如果CDH6的外部表使用的是非默认路径，则只需要替换nameservice即可
                    if (d.contains("user/hive/warehouse")) {
//                        d = d.replace("user/hive/warehouse", "warehouse/tablespace/external/hive");
                        //  库表重命名后对应的库表location也需要进行替换
                        d = d.replace(sourceDB, targetDB).replace(sourceTB, targetTB);
                        logger.info("6-7,目标表路径为："+d);
//                        StringUtils.printstrX_X("同步到cdp7的外表，则目标表路径为：warehouse/tablespace/external/hive");
                    }
                }
            } else if ("cdp7".equalsIgnoreCase(sourceVersion) && !"cdp7".equalsIgnoreCase(targetVersion)) {
                //  cdp7同步到非cdp7集群
                if (tableType.equalsIgnoreCase("MANAGED_TABLE")) {
                    d = d.replace("warehouse/tablespace/managed/hive", "user/hive/warehouse");
                    //  库表重命名后对应的库表location也需要进行替换
                    d = d.replace(sourceDB, targetDB).replace(sourceTB, targetTB);
                    StringUtils.printstrX_X("cdp7同步到非cdp7的内表，则目标表路径为：user/hive/warehouse");
                } else if (tableType.equalsIgnoreCase("EXTERNAL_TABLE")) {
                    if (d.contains("warehouse/tablespace/external/hive")) {
                        d = d.replace("warehouse/tablespace/external/hive", "user/hive/warehouse");
                        //  库表重命名后对应的库表location也需要进行替换
                        d = d.replace(sourceDB, targetDB).replace(sourceTB, targetTB);
                        StringUtils.printstrX_X("cdp7同步到非cdp7的外表，则目标表路径为：user/hive/warehouse");
                    }
                }
            } else {
                //  库表重命名后对应的库表location也需要进行替换
                d = d.replace(sourceDB, targetDB).replace(sourceTB, targetTB);
            }
            return d;
        } catch (Exception e) {
            logger.error("se=" + se + " sLocation" + sLocation);
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 将分区集合进行转换，提取出partition的value，即分区值作为map的key
     *
     * @param partitions
     * @return 返回转换后的分区集合
     */
    public HashMap<String, Partition> transforPartitions(List<Partition> partitions) {
        HashMap<String, Partition> map = new HashMap<String, Partition>();

        for (Partition partition : partitions) {
            map.put(partition.getValues().toString(), partition);
        }
        return map;
    }

    public SubTask createAndStoreNewSubTask(String sourceCluster, String sourceDB, String sourceTB, String targetCluster, String targetDB, String targetTB, String sLocation, String dLocation, int mainTaskId) {
        if ("true".equalsIgnoreCase(PropUtil.getProValue(Constant.DISTCP_SNAPSHOT))) {
            //  基于快照的数据同步需要替换sLocation路径为包含快照目录的路径
            sLocation = HDFSUtil.getSnapshotLocation(sLocation);
        }
        SubTask subTask = new SubTask(
                sourceCluster,
                sourceDB, sourceTB,
                targetCluster,
                targetDB,
                targetTB,
                Constant.TASK_STATUS_WAIT,
                0,
                Constant.TASK_STATUS_WAIT,
                sLocation,
                dLocation,
                "",
                0,
                mainTaskId);
        int id = subTaskService.insert(subTask);
        if (id > 0) {
            subTask.setId(id);
            return subTask;
        }
        return null;
    }

}
