package com.spdb.hive.sync.start;

import com.spdb.hive.sync.concurrent.SyncThread;
import com.spdb.hive.sync.concurrent.TableQueue;
import com.spdb.hive.sync.concurrent.authKrb5Thread;
import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.distcp.HdfsFileDistcp;
import com.spdb.hive.sync.entity.MainTask;
import com.spdb.hive.sync.entity.SubTask;
import com.spdb.hive.sync.entity.SyncEntity;
import com.spdb.hive.sync.service.MainTaskService;
import com.spdb.hive.sync.service.SubTaskService;
import com.spdb.hive.sync.util.auth.Authkrb5;
import com.spdb.hive.sync.util.date.DateUtil;
import com.spdb.hive.sync.util.hdfs.HDFSUtil;
import com.spdb.hive.sync.util.hive.TableUtil;
import com.spdb.hive.sync.util.parser.OptionParserUtil;
import com.spdb.hive.sync.util.progress.ProgressUtil;
import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.spdb.hive.sync.util.LogUtil;
/**
 * project：hive-test
 * package：com.spdb.hive.replication.start
 * author：zhouxh
 * time：2021-03-30 9:28
 * description：启动入口
 */
public class Start {
    // Logger和LoggerFactory导入的是org.slf4j包
    //ceshi
    private static Logger logger = LogUtil.getLogger();
    private static MainTaskService mainTaskService = new MainTaskService();
    private static SubTaskService subTaskService = new SubTaskService();

    public static void main(String[] args) {
        int retype = 0;
        if (args != null && args.length > 1) {


            List<String> ipAndCommandId = Arrays.asList(args[args.length - 1].split(":"));

            //设置全局变量log4j的后半路径
            System.setProperty("COMMANDID_SALMON_LOG", ipAndCommandId.get(1) + "_salmon.log");


            retype = 0;
            if (args.length > 1) {
                String[] tmpArgs = new String[args.length - 1];
                //保存args的最后参数,判断commandid和kerbros
                //  参数解析
                System.arraycopy(args, 0, tmpArgs, 0, args.length - 1);
                int flg = OptionParserUtil.optionParse(tmpArgs);
                if (flg < 0) {
                    logger.error("输入参数有误，请重新输入");
                    System.exit(-1);
                    return;
                }
                //  数据同步
                retype = run(args);
//            System.out.println(retype);

                //同步文件
                if (tmpArgs[4].contains("/")) {
                    try {
                        retype = runFile(tmpArgs);
                        logger.info("文件同步,参数" + Arrays.toString(tmpArgs));
                        resultURL(ipAndCommandId.get(0), ipAndCommandId.get(1), "success");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    //同步表
                    try {
                        retype = run(tmpArgs);
                        logger.info("库表同步,参数" + Arrays.toString(tmpArgs));
                        resultURL(ipAndCommandId.get(0), ipAndCommandId.get(1), "failed");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                //系统退出
                System.exit(retype);

            } else {
                logger.error("输入参数有误，请重新输入");
                usage();
                System.exit(-1);
            }
        }
    }
    public static int runFile(String[] args) {
        HdfsFileDistcp hdfsFileDistcp = new HdfsFileDistcp();
        hdfsFileDistcp.syncFileData(args[0], args[1], args[4], args[5]);
        return 0;
    }
    public static int run(String[] args) {
        int retype = 0;
        //  如果是手动kill主任务
        if (PropUtil.getProValue(Constant.TASK_STATUS_KILL) != null) {
            retype = kill(Integer.valueOf(PropUtil.getProValue(Constant.TASK_STATUS_KILL)));
            return retype;
        }
        logger.info("数据同步任务开始");
        long start = System.currentTimeMillis();
        Authkrb5.authkrb5Default();
        MainTask mainTask = null;
        List<SyncEntity> ses = null;
        //  第一个参数是源集群名称
        String sourceCluster = null;
        //  第二个参数是目标集群名称
        String targetCluster = null;
        //  获取运行模式
        String mode = PropUtil.getProValue(Constant.SYNC_MODE);
        if (mode != null && Constant.RESUME_MODE.equalsIgnoreCase(mode)) {
            //  断点续传模式
            //  根据mainTaskId查询main_task对象
            mainTask = mainTaskService.queryById(Integer.valueOf(PropUtil.getProValue(Constant.RESUME_MODE_ID)));
            if (mainTask == null) {
                logger.error("当前传入的主任务ID无法查询到相关记录，请传入有效的主任务ID");
                return -1;
            }
            ses = resume(mainTask);
            sourceCluster = mainTask.getSourceCluster();
            targetCluster = mainTask.getTargetCluster();
        } else {
            if (args.length >= 3) {
                //  新任务模式
                //  第一个参数是源集群名称
                sourceCluster = args[0];
                //  第二个参数是目标集群名称
                targetCluster = args[1];
                //  获得所有需要同步的数据表名
                ses = TableUtil.getSyncEntitiesV3(args);
                //  判断是否为极速模式（入参的最后一个参数项）
                if (Constant.SPEED_MODE.equalsIgnoreCase(mode)) {
                    //  如果为极速模式则需要判断目标集群是否存在数据库，如果存在则给出提示程序退出
                    int i = TableUtil.dbExists(targetCluster, ses);
                    if (i == 1) {
                        return 0;
                    }
                }
            } else {
                logger.error("参数个数有误，请重新输入");
                usage();
                return -1;
            }
        }
        if (ses != null && ses.size() > 0) {
            //  非断点续传模式需要生成新的主任务，断点续传模式不用
            if (!Constant.RESUME_MODE.equalsIgnoreCase(mode)) {
                //  生成主任务记录
                mainTask = new MainTask(StringUtils.join(args, " "), sourceCluster, targetCluster, "running");
                //  主任务记录入库存储
                int id = mainTaskService.insert(mainTask);
                //  如果返回的自增主键有问题则程序直接退出
                if (id < 1) {
                    logger.error("主任务入库异常，程序退出");
                    return -1;
                }
                //  设置数据库返回的主键
                mainTask.setId(id);
            }
            int mainTaskId = mainTask.getId();
            logger.info("当前主任务ID：" + mainTaskId);
            String snapshotName = null;
            //  基于快照的数据同步
            if ("true".equalsIgnoreCase(PropUtil.getProValue(Constant.DISTCP_SNAPSHOT))) {
                logger.info("本次数据同步基于快照进行");
                //  创建快照，返回源集群每张表级别目录对应的快照目录集合
                snapshotName = HDFSUtil.createSnapshot(mainTaskId, sourceCluster, targetCluster, ses);
                if (snapshotName == null) {
                    //logger.error(sourceCluster + "快照名称为null，进程退出");
                    mainTask.setMainTaskStatus(Constant.TASK_STATUS_FINISH);
                    mainTask.setMainTaskResult(Constant.TASK_STATUS_FAIL);
                    mainTask.setStopTime(DateUtil.getDate());
                    mainTaskService.update(mainTask);
                    return 0;
                }
            }
            //  所有表初始化到进度输出集合
            ProgressUtil.putNew(ses);
            ProgressUtil.setMainTaskId(mainTaskId);
            logger.info("需要同步的数据表个数：" + ses.size() + "，明细为：" + ses.toString());
            //  目标集群目标数据库校验，不存在则创建
            TableUtil.validateTargetDB(mainTask.getTargetCluster(), ses);
            //  将数据表初始化到队列中
            TableQueue.addSE(ses);
            //  默认并发数
            int concurrent = 3;
            String concurrentStr = PropUtil.getProValue("sync.concurrent");
            if (concurrentStr != null && NumberUtils.isDigits(concurrentStr)) {
                concurrent = Integer.valueOf(concurrentStr);
            }

            //  创建线程池
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    concurrent,
                    concurrent,
                    0,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());

            //开始定时认证用户线程
            authKrb5Thread authKrb5Thread = new authKrb5Thread();
            executor.submit(authKrb5Thread);

            for (int i = 0; i < concurrent; i++) {
                SyncThread myThread = new SyncThread(mainTask, mode);
                executor.submit(myThread);
            }
            //  关闭线程池
            executor.shutdown();
            logger.info("调用线程池shutdown");
            //  循环检测子线程是否都结束
            while (true) {
                if (executor.isTerminated()) {
                    logger.info("所有子线程都结束");
                    break;
                }
                try {
                    Thread.sleep(1000 * 3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //  当用户手动kill主任务时数据库中的main_task_result字段被更新成了kill，所有还未处理的数据表不再处理，正在处理的数据表会继续处理，子线程不再接收新的SyncEntity
            //  所有正在处理的数据表处理完后代码会执行到此处，此时应该从数据库中根据mainTaskId获得当前mainTask的最新字段值，
            //  如果main_task_status字段为finish且main_task_result字段为kill则不再更新mainTask，但是已经同步成功的子任务文件一致性比对以及源集群快照的清理是需要做的
            mainTask = mainTaskService.queryById(mainTaskId);
            if (Constant.TASK_STATUS_FINISH.equalsIgnoreCase(mainTask.getMainTaskStatus())
                    && Constant.TASK_STATUS_KILL.equalsIgnoreCase(mainTask.getMainTaskResult())) {
                logger.info("当前主任务被用户手动kill，任务ID：" + mainTaskId);
                //  获得主任务下成功状态的子任务
                ArrayList<SubTask> successSubTasks = subTaskService.querySuccessByMainTaskId(mainTaskId);
                //  文件一致性检测
                HDFSUtil.consistencyCheck(sourceCluster, targetCluster, snapshotName, successSubTasks);
                //  清理源集群快照
                if ("true".equalsIgnoreCase(PropUtil.getProValue(Constant.DISTCP_SNAPSHOT))) {
                    HDFSUtil.deleteSnapshots(mainTaskId);
                }
                return -1;
            }
            //  所有子线程运行结束，回到主线程
            //  更新主任务状态和结果，以及结束时间
            //  获得主任务下所有子任务记录数和成功状态的子任务数
            int totalSubTask = subTaskService.queryCntByMainTaskId(mainTaskId);
            ArrayList<SubTask> successSubTasks = subTaskService.querySuccessByMainTaskId(mainTaskId);
            int successSubTask = successSubTasks.size();
            //  数据同步失败的子任务
            ArrayList<SubTask> failedSubTasks = subTaskService.queryFailedCntByMainTaskId(mainTaskId);

            StringBuilder failedTables = new StringBuilder(String.valueOf(failedSubTasks.size()));
            if (failedSubTasks.size() > 0) {
                //如果存在失败任务，则设置返回值为-1
                retype = -1;
                failedTables.append(" [ ");
                for (SubTask subTask : failedSubTasks) {
                    failedTables.append(subTask.getSourceDB() + "." + subTask.getSourceTB() + " ");
                }
                failedTables.append("]");
            }

            mainTask.setMainTaskStatus(Constant.TASK_STATUS_FINISH);
            mainTask.setStopTime(DateUtil.getDate());
            String result = "";
            if (totalSubTask == successSubTask) {
                result = Constant.TASK_STATUS_SUCCESS;
            } else if (totalSubTask > 0 && totalSubTask > successSubTask) {
                result = Constant.TASK_STATUS_WARN;
            } else if (totalSubTask > 0 && successSubTask == 0) {
                result = Constant.TASK_STATUS_FAIL;
            }
            mainTask.setMainTaskResult(result);
            mainTaskService.update(mainTask);
            //  源数据库UDF函数检测
            //  TableUtil.udfCheck(mainTask.getSourceCluster(), ses);
            StringBuilder msg = new StringBuilder();

            //  如果是resume模式运行的话ses中的集合是去除了历史运行成功的，所有这里匹配到的总数据表个数需要重新计算
            int totalTables = ses.size();
            if (mode != null && Constant.RESUME_MODE.equalsIgnoreCase(mode)) {
                List<SyncEntity> t = TableUtil.getSyncEntitiesV3(mainTask.getMainTaskArgs().split(" "));
                if (t != null) {
                    totalTables = t.size();
                }
            }
            msg.append("数据同步任务运行结束，主任务ID：")
                    .append(mainTaskId)
                    .append("，匹配到的数据表总数 ").append(totalTables)
                    .append("，元数据发生变化的表数量 ").append(totalSubTask)
                    .append("，元数据未发生变化的表数量 ").append((totalTables - totalSubTask))
                    .append("，数据同步成功的表数量 ").append(successSubTask)
                    .append("，数据同步失败的表数量 ").append(failedTables.toString())
                    .append("，主任务执行结果 ").append(result)
                    .append("，" + DateUtil.getIntervalFormat((System.currentTimeMillis() - start) / 1000));

            logger.info(msg.toString());
            String fileName = mainTaskId + "-" + start + ".result";
            ProgressUtil.writeResult(fileName, msg.toString());
            //  文件一致性检测
            if (PropUtil.getProValue(Constant.CHECKSUM) != null) {
                HDFSUtil.consistencyCheck(sourceCluster, targetCluster, snapshotName, successSubTasks);
            }
            //  清理源集群快照
            if ("true".equalsIgnoreCase(PropUtil.getProValue(Constant.DISTCP_SNAPSHOT))) {
                HDFSUtil.deleteSnapshots(mainTaskId);
            }
        } else {
            logger.warn("需要同步的数据表个数为0个，请检查输入参数");
            retype = -1;
        }
        return retype;
    }

    /**
     * 断点续传模式
     *
     * @param mainTask
     */
    public static List<SyncEntity> resume(MainTask mainTask) {
        if (mainTask != null) {
            //  断点续传模式需要再次解析上一次主任务的args参数，封装到properties中
            int flg = OptionParserUtil.optionParse(mainTask.getMainTaskArgs().split(" "));
            if (flg < 0) {
                return null;
            }
            //  获得所有需要同步的数据表名
            List<SyncEntity> ses = TableUtil.getSyncEntitiesV3(mainTask.getMainTaskArgs().split(" "));
            //  获得上一次运行主任务时生成的sub_task
            ArrayList<SubTask> subTasks = subTaskService.queryByMainTaskId(mainTask.getId());
            //  遍历
            for (SubTask subTask : subTasks) {
                //  如果sub_task的元数据和主数据都同步成功则从本次待处理列表移除
                if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(subTask.getMetadataSyncStatus())
                        && Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(subTask.getMaindataSyncStatus())) {
                    ses.remove(new SyncEntity(subTask.getSourceCluster(), subTask.getSourceDB(), subTask.getSourceTB(),
                            subTask.getTargetCluster(), subTask.getTargetDB(), subTask.getTargetTB()));
                    logger.info("上一次的同步过程中同步成功，本次断点续传跳过 " + subTask.getBriefInfo());
                } else {
                    logger.info("重置子任务主数据同步重试次数为0 " + subTask.getBriefInfo());
                    //  只要还保留在本次待处理列表中的，重置主数据同步重试次数为0
                    subTaskService.resetMaindataSyncRetry(subTask.getId());
                }
            }
            //  断点续传模式需要将主任务状态更新为running
            mainTask.setMainTaskStatus(Constant.TASK_STATUS_RUNNING);
            mainTaskService.update(mainTask);
            return ses;
        }
        return null;
    }

    public static int kill(int mainTaskId) {
        //  查看当前主任务运行状态和结果
        MainTask mainTask = mainTaskService.queryById(mainTaskId);
        if (mainTask == null) {
            logger.error("当前传入的主任务ID无法查询到相关记录，请传入有效的主任务ID");
            return -1;
        }
        if (Constant.TASK_STATUS_FINISH.equalsIgnoreCase(mainTask.getMainTaskStatus())) {
            logger.info("当前主任务已经运行结束，无需手动kill");
            return 0;
        } else {
            //  更新main_task_status字段为finish
            mainTask.setMainTaskStatus(Constant.TASK_STATUS_FINISH);
            //  更新main_task_result字段为kill
            mainTask.setMainTaskResult(Constant.TASK_STATUS_KILL);
            mainTask.setStopTime(DateUtil.getDate());
            int flag = mainTaskService.update(mainTask);
            if (flag > 0) {
                logger.info("当前主任务已经kill，正在处理的数据表处理完成后进程退出，未处理的处理表不再处理");
                return 0;
            } else {
                logger.error("当前主任务kill失败，请查看报错日志");
                return -1;
            }
        }
    }

    public static void usage() {
        System.out.println("请指定源集群名称 目标集群名称 需要同步的数据库和数据表 [重命名后的数据库和数据表]");
        System.out.println("\tsource target -s multi|single database.table database1.table1");
        System.out.println("\teg: yanlian dongcha -s multi zxh_warehouse.* zxh_warehouse_snapshot{suffix=_sf} -D sync.concurrent=1 -D distcp.retry.num=2 -overwrite -bandwidth 100 -maps 3");
    }
}
