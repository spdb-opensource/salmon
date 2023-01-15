package com.spdb.hive.sync.concurrent;

import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.entity.MainTask;
import com.spdb.hive.sync.entity.SubTask;
import com.spdb.hive.sync.entity.SyncEntity;
import com.spdb.hive.sync.service.DistCPService;
import com.spdb.hive.sync.service.HMSCService;
import com.spdb.hive.sync.service.MainTaskService;
import com.spdb.hive.sync.service.SubTaskService;
import com.spdb.hive.sync.util.auth.Authkrb5;
import com.spdb.hive.sync.util.hive.HMSCUtil;
import com.spdb.hive.sync.util.progress.ProgressUtil;
import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.hive.metastore.MyHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.ArrayList;
import com.spdb.hive.sync.util.LogUtil;
/**
 * project：hive-sync
 * package：com.spdb.hive.sync.concurrent
 * author：zhouxh
 * time：2021-04-19 9:52
 * description：
 */
public class SyncThread implements Runnable {
    private HMSCService hmscService = new HMSCService();
    private DistCPService distCPService = new DistCPService();
    private MainTaskService mainTaskService = new MainTaskService();
    private SubTaskService subTaskService = new SubTaskService();
    //  主任务对象
    private MainTask mainTask;
    //  运行模式
    private String mode;

    private final static Logger logger = LogUtil.getLogger();

    public SyncThread(MainTask mainTask, String mode) {
        this.mainTask = mainTask;
        this.mode = mode;
    }

    @Override
    public void run() {
        //  主数据同步重试次数
        int maindataRetryNum = 3;
        String maindataRetryNumStr = PropUtil.getProValue(Constant.DISTCP_RETRY_NUM);
        if (maindataRetryNumStr != null && NumberUtils.isDigits(maindataRetryNumStr)) {
            maindataRetryNum = Integer.valueOf(maindataRetryNumStr);
        }
        while (true) {
            //  获得需要同步的表名
            SyncEntity se = TableQueue.getSE();
            if (se != null) {
                logger.info("当前线程" + Thread.currentThread().getName() + "处理" + se);
                Authkrb5.authkrb5Default();
                //  这时候拿到的item有可能是主数据同步错误之后再放回去的，这样的话在sub_task中已经有记录了
                //  根据主任务id，数据库名，表名，从sub_task进行查询
                SubTask subTask = subTaskService.queryByMDBTable(mainTask.getId(), se.getSourceCluster(), se.getSourceDB(), se.getSourceTB(), se.getTargetCluster(), se.getTargetDB(), se.getTargetTB());
                if (subTask == null) {
                    //  元数据比对，返回子任务对象ID
                    int subTaskId = hmscService.syncMetadata(mainTask.getId(), se, mode);
                    if (subTaskId > 0) {
                        //  数据同步进度输出
                        printProgress();
                        //  进行主数据同步
//                       String tabletype="";
//                        try {
//                            MyHiveMetaStoreClient shmsc = null;
//                            shmsc = HMSCUtil.getHMSC( se.getSourceCluster() + ".hive.conf");
//                            Table sTable = shmsc.getTable(se.getSourceDB(),se.getSourceTB());
//                            tabletype=sTable.getTableType();
//                        } catch (TException e) {
//                            e.printStackTrace();
//                        }
//                        if(tabletype.equalsIgnoreCase("VIRTUAL_VIEW")){
//                            subTask.setMaindataSyncStatus("success");
//                            subTaskService.update(subTask);
//                        }
//                        else{
                        distCPService.syncMainData(subTaskId);
                        //  数据同步进度输出
                        printProgress();
                    } else {
                        //  这里再次通过queryByMDBTable去查询subTask是因为syncMetadata中对subTask进行了update
                        subTask = subTaskService.queryByMDBTable(mainTask.getId(), se.getSourceCluster(), se.getSourceDB(), se.getSourceTB(), se.getTargetCluster(), se.getTargetDB(), se.getTargetTB());
                        if (subTask != null) {
                            //  数据同步进度输出
                            printProgress();
                        }
                    }
                } else {
                    //  元数据同步成功，主数据同步失败，且主数据重试次数<3
                    if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(subTask.getMetadataSyncStatus())
                            && Constant.TASK_STATUS_FAIL.equalsIgnoreCase(subTask.getMaindataSyncStatus())
                            && subTask.getMaindataSyncRetry() < maindataRetryNum) {
                        //  这里针对元数据同步成功，主数据同步失败的子任务目前是直接对主数据再次同步，这是为了实现同一次数据同步过程中主数据同步失败重试机制
                        //  但是针对resume模式，有可能用户希望恢复几天前的数据同步任务，如果这几天过程中表元数据又发生了变化，那么直接同步主数据就有问题了
                        //  考虑到这里，即使在同一次数据同步任务中，主数据同步失败后放入TableQueue尾部等待再次重试，如果这期间源表的主数据发生变化了，直接同步也是有问题的
                        //  目前考虑的做法是针对元数据同步成功，主数据同步失败且主数据同步尝试次数<配置的次数的子任务，
                        //  也先进行元数据同步，返回0说明元数据没变化，那么直接同步主数据即可，如果返回结果>0则说明元数据又变化了，且生成了新的子任务，直接根据新的子任务进行主数据同步
                        int subTaskId = hmscService.syncMetadata(mainTask.getId(), se, mode);
                        if (subTaskId > 0) {
                            //  subTaskId>0说明本次元数据又发生变化了，生成了新的子任务，根据新的子任务ID进行主数据同步
                            //  进行主数据同步
                            distCPService.syncMainData(subTaskId);
                        } else if (subTaskId == 0) {
                            //  subTaskId == 0说明本次元数据未发生变化，只需要把主数据同步到目标集群即可
                            //  进行主数据同步
                            distCPService.syncMainData(subTask.getId());
                            //  获得数据库中该条sub_task最新对象
                            subTask = subTaskService.queryById(subTask.getId());
                            //  重试次数加1
                            subTask.setMaindataSyncRetry(subTask.getMaindataSyncRetry() + 1);
                            //  更新数据库
                            subTaskService.update(subTask);
                        }
                        //  数据同步进度输出
                        printProgress();
                    } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(subTask.getMetadataSyncStatus())
                            && Constant.TASK_STATUS_WAIT.equalsIgnoreCase(subTask.getMaindataSyncStatus())) {
                        //  使用resume模式的时候处理元数据同步成功但是主数据等待同步的子任务
                        //  因为使用的的是resume模式，所以这里会对元数据再次比对
                        //  元数据比对，返回子任务对象ID
                        int subTaskId = hmscService.syncMetadata(mainTask.getId(), se, mode);
                        if (subTaskId > 0) {
                            //  subTaskId>0说明本次元数据又发生变化了，生成了新的子任务，根据新的子任务ID进行主数据同步
                            //  进行主数据同步
                            distCPService.syncMainData(subTaskId);
                        } else if (subTaskId == 0) {
                            //  subTaskId == 0说明本次元数据未发生变化，只需要把主数据同步到目标集群即可
                            //  进行主数据同步
                            distCPService.syncMainData(subTask.getId());
                        }
                        //  数据同步进度输出
                        printProgress();
                    } else if (subTask.getMaindataSyncRetry() >= maindataRetryNum) {
                        logger.warn("当前子任务主数据同步重试次数超过配置的次数，本次同步跳过，" + subTask.getBriefInfo());
                    }
                }
            } else {
                logger.info("当前线程" + Thread.currentThread().getName() + "无法从tablequeue中获得待处理的数据表");
                break;
            }

            //  每处理完一张数据表，都从数据库中获得当前主任务最新记录
            mainTask = mainTaskService.queryById(mainTask.getId());
            if (Constant.TASK_STATUS_FINISH.equalsIgnoreCase(mainTask.getMainTaskStatus())
                    && Constant.TASK_STATUS_KILL.equalsIgnoreCase(mainTask.getMainTaskResult())) {
                logger.info("当前主任务被用户手动kill，任务ID：" + mainTask.getId() + "，当前子线程退出");
                break;
            }
        }
    }

    //根据subtaskid获取任务，获取是否是view同步，如果是返回false，else返回true

    public void printProgress() {
        ArrayList<SubTask> subTasks = subTaskService.queryByMainTaskId(mainTask.getId());
        ProgressUtil.printProgress(subTasks);
    }
}
