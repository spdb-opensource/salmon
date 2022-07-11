package com.spdb.hive.sync.service;

import com.spdb.hive.sync.concurrent.TableQueue;
import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.entity.SubTask;
import com.spdb.hive.sync.entity.SyncEntity;
import com.spdb.hive.sync.util.date.DateUtil;
import com.spdb.hive.sync.util.hive.HMSCUtil;
import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MyHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datanucleus.cache.NullLevel2Cache;

import java.io.IOException;
import java.util.ArrayList;


/**
 * project：hive-sync
 * package：com.spdb.hive.sync.service
 * author：zhouxh
 * time：2021-04-15 9:14
 * description：
 */
public class DistCPService {

    private SubTaskService subTaskService = new SubTaskService();

    private final static Logger logger = LogManager.getLogger(DistCPService.class);

    public void syncMainData(int subTaskId) {


        SubTask subTask = null;
        Job job = null;
        try {
            subTask = subTaskService.queryById(subTaskId);

            MyHiveMetaStoreClient shmsc = null;
            shmsc = HMSCUtil.getHMSC(subTask.getSourceCluster() + ".hive.conf");
            Table sTable = shmsc.getTable(subTask.getSourceDB(), subTask.getSourceTB());
            String tableType = sTable.getTableType();
            if (tableType.equalsIgnoreCase("VIRTUAL_VIEW")){
                subTask.setMaindataSyncStatus(Constant.VIEW_MAINDATA_NDO);
                subTask.setMaindataSyncStop(DateUtil.getDate());
                subTaskService.update(subTask);
                return;
            }
            String sourcePath = subTask.getMaindataSyncSourcePath();
            String targetPath = subTask.getMaindataSyncTargetPath();

            logger.info("开始同步主数据：" + sourcePath + "\t" + targetPath);

            Configuration conf = new Configuration();
            //  distcp nameservices配置文件路径，包含所有集群的nameservice解析
            conf.addResource(PropUtil.getProValue(Constant.DISTCP_NAMESERVICES_CONF));
            //  distcp所提交的yarn集群配置文件路径，即yarn-site.xml配置文件路径
            conf.addResource(PropUtil.getProValue(Constant.DISTCP_YARN_CONF));
            conf.set("mapreduce.job.hdfs-servers.token-renewal.exclude", getNameservices(sourcePath, targetPath));
            ArrayList<Path> sourcePaths = new ArrayList<Path>();

            sourcePaths.add(new Path(sourcePath));
            Path des = new Path(targetPath);

            DistCpOptions.Builder builder = new DistCpOptions.Builder(sourcePaths, des);

            //  解析distcp参数
            if (PropUtil.getProValue(Constant.DISTCP_UPDATE) != null ) {
                //  等同于命令行中的-update参数
                builder.withSyncFolder(true);
            }
            if (PropUtil.getProValue(Constant.DISTCP_OVERWRITE) != null) {
                //  等同于命令行中的-overwrite参数
                builder.withOverwrite(true);
            }
            if (PropUtil.getProValue(Constant.DISTCP_DELETE) != null ) {
                //  等同于命令行中的-delete参数
                builder.withDeleteMissing(true);
            }
            if (PropUtil.getProValue(Constant.DISTCP_MAPS) != null) {
                //  等同于命令行中的-m参数
                builder.maxMaps(Integer.valueOf(PropUtil.getProValue(Constant.DISTCP_MAPS)));
            }
            if (PropUtil.getProValue(Constant.DISTCP_BANDWIDTH) != null) {
                //  等同于命令行中的-bandwidth参数
                builder.withMapBandwidth(Integer.valueOf(PropUtil.getProValue(Constant.DISTCP_BANDWIDTH)));
            }
            if (PropUtil.getProValue(Constant.DISTCP_SKIPCRCCHECK) != null) {
                //  等同于命令行中的skipCRC=false，进行CRC文件内容校验，防止文件名和大小一致数据内容不一致时没有进行比对
                builder.withCRC(false);
            }
            if (PropUtil.getProValue(Constant.DYNAMIC) != null) {
                builder.withCopyStrategy("dynamic");
            }


            //  封装option
            DistCpOptions options = builder.build();

            logger.info("当前生效的distcp参数：" + options.toString());
            System.out.println("当前生效的distcp参数：" + options.toString());
            System.out.println("======================当前生效的distcp配置：==================" + conf.toString());
            //  创建distcp
            DistCp distCp = new DistCp(conf, options);
            job = distCp.createAndSubmitJob();
            logger.info("作业已经提交，" + job.getJobID() + "，等待作业运行结束");

            //  更新子任务对象的maindata_sync_args、maindata_sync_job_id、maindata_sync_job_status、maindata_sync_start、update_time
            subTask.setMaindataSyncArgs(options.toString());
            subTask.setMaindataSyncStatus(Constant.TASK_STATUS_RUNNING);
            subTask.setMaindataSyncJobId(job.getJobID().toString());
            subTask.setMaindataSyncJobStatus(job.getJobState().name());
            subTask.setMaindataSyncStart(DateUtil.getDate());
            subTaskService.update(subTask);
            //  等待distcp任务运行结束
            try{
                distCp.waitForJobCompletion(job);
            }
            catch (Exception e){
                e.printStackTrace();
                logger.error(e.getMessage());
                if (subTask != null) {
                    try {
                        subTask.setMaindataSyncStatus(Constant.TASK_STATUS_FAIL);
                        if (job != null) {
                            subTask.setMaindataSyncJobStatus(job.getJobState().name());
                        }
                        subTask.setMaindataSyncStop(DateUtil.getDate());
                        subTaskService.update(subTask);

                        //  如果MR作业执行结果不是SUCCEEDED则放回TableQueue
                        //TableQueue.add(subTask.getDatabaseName() + "." + subTask.getTableName());
//                        TableQueue.addSE(new SyncEntity(subTask.getSourceCluster(), subTask.getSourceDB(), subTask.getSourceTB(),
//                                subTask.getTargetCluster(), subTask.getTargetDB(), subTask.getTargetTB()));
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                }
                return;
            }


            //  更新子任务对象的maindata_sync_status、maindata_sync_job_status、maindata_sync_stop、update_time
            String result = job.getJobState().name();
            subTask.setMaindataSyncJobStatus(result);
            if (Constant.TASK_STATUS_SUCCEEDED.equalsIgnoreCase(result)) {
                subTask.setMaindataSyncStatus(Constant.TASK_STATUS_SUCCESS);
            } else {
                subTask.setMaindataSyncStatus(Constant.TASK_STATUS_FAIL);
            }
            subTask.setMaindataSyncStop(DateUtil.getDate());
            subTaskService.update(subTask);

            logger.info("作业运行结束，" + job.getJobID() + "，结果状态：" + job.getJobState());

            if (!Constant.TASK_STATUS_SUCCEEDED.equalsIgnoreCase(result)) {
                //  如果MR作业执行结果不是SUCCEEDED则放回TableQueue
                //TableQueue.add(subTask.getDatabaseName() + "." + subTask.getTableName());
                TableQueue.addSE(new SyncEntity(subTask.getSourceCluster(), subTask.getSourceDB(), subTask.getSourceTB(),
                        subTask.getTargetCluster(), subTask.getTargetDB(), subTask.getTargetTB()));
            } else {
                //  先判断元数据和主数据同步状态
                if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(subTask.getMetadataSyncStatus())
                        && Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(subTask.getMaindataSyncStatus())) {
                    //  更新所有之前这个目标集群这个库这张表的异常子任务（元数据同步成功主数据同步状态为wait的）sub_task_status为discard
                    subTaskService.updateAbnormalSubTask(subTask);
                    //  更新同一个主数据同步任务中该数据库数据表的失败子任务（元数据同步成功主数据同步失败）sub_task_status为discard，
                    //  因为在使用resume模式时可能因为元数据的变化生成2个对同一张表同步的子任务，这样统计主任务的result时就不准确了，
                    //  所以只要在同一个主数据同步中这个表成功了，那就更新之前失败的，标记discard不参与后期统计
                    subTaskService.updateFailSubTask(subTask);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            //  更新子任务对象的maindata_sync_status、
            if (subTask != null) {
                try {
                    subTask.setMaindataSyncStatus(Constant.TASK_STATUS_FAIL);
                    if (job != null) {
                        subTask.setMaindataSyncJobStatus(job.getJobState().name());
                    }
                    subTask.setMaindataSyncStop(DateUtil.getDate());
                    subTaskService.update(subTask);

                    //  如果MR作业执行结果不是SUCCEEDED则放回TableQueue
                    //TableQueue.add(subTask.getDatabaseName() + "." + subTask.getTableName());
                    TableQueue.addSE(new SyncEntity(subTask.getSourceCluster(), subTask.getSourceDB(), subTask.getSourceTB(),
                            subTask.getTargetCluster(), subTask.getTargetDB(), subTask.getTargetTB()));
                } catch (IOException e1) {
                    e1.printStackTrace();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    /**
     * @param url hdfs全路径
     * @return 获得hdfs全路径的nameservice
     */
    public String getNameservice(String url) {
        String tmpUri = url.substring(url.indexOf("hdfs://") + 7);
        String host = tmpUri.substring(0, tmpUri.indexOf("/"));
        return host;
    }

    public String getNameservices(String source, String target) {
        String sourceUri = source.substring(source.indexOf("hdfs://") + 7);
        String sourceNameservice = sourceUri.substring(0, sourceUri.indexOf("/"));
        String targetUri = target.substring(target.indexOf("hdfs://") + 7);
        String targetNameservice = targetUri.substring(0, targetUri.indexOf("/"));
        return sourceNameservice + "," + targetNameservice;
    }

}
