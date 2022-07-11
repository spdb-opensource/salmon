package com.spdb.hive.sync.util.progress;

import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.entity.SubTask;
import com.spdb.hive.sync.entity.SyncEntity;
import com.spdb.hive.sync.util.date.DateUtil;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * project：hive-sync
 * package：com.spdb.hive.sync.util.progress
 * author：zhouxh
 * time：2021-04-26 15:17
 * description：
 */
public class ProgressUtil {

    private final static Logger logger = LogManager.getLogger(ProgressUtil.class);

    private static int mainTaskId = 0;

    private static HashMap<String, SubTask> items = new HashMap<String, SubTask>();

    public synchronized static void put(String key, SubTask task) {
        items.put(key, task);
    }

    public static void setMainTaskId(int id) {
        mainTaskId = id;
    }

    public static void putNew(List<SyncEntity> ses) {
        for (SyncEntity se : ses) {
            put(se.getSourceDB() + "." + se.getSourceTB(), null);
        }
    }

    /**
     * 数据同步进度输出
     */
    public synchronized static void printProgress() {
        try {
            if (items != null) {
                StringBuilder msg = new StringBuilder("\r\n************************数据同步进度************************\r\n");
                HashMap<String, SubTask> map = new HashMap<String, SubTask>();
                map.putAll(items);

                Set<String> keys = map.keySet();
                SubTask task = null;
                String metadataSyncStatus = null;
                String maindataSyncStatus = null;
                for (String key : keys) {
                    msg.append("数据同步主任务ID：").append(mainTaskId).append("，数据表").append(key);
                    if (map.get(key) == null) {
                        msg.append("还未开始同步");
                    } else {
                        task = map.get(key);
                        metadataSyncStatus = task.getMetadataSyncStatus();
                        maindataSyncStatus = task.getMaindataSyncStatus();
                        if (Constant.TASK_STATUS_WAIT.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_WAIT.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("还未开始同步");
                        } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_WAIT.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，主数据还未同步");
                        } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_RUNNING.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，主数据正在同步");
                        } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，主数据同步成功");
                        } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_FAIL.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，主数据同步失败");
                        } else if (Constant.TASK_STATUS_FAIL.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_WAIT.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步失败，主数据还未同步");
                        } else if (Constant.TASK_STATUS_FAIL.equalsIgnoreCase(metadataSyncStatus) && Constant.VIEW_MAINDATA_NDO.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步失败，视图无需同步主数据，");
                        }else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.VIEW_MAINDATA_NDO.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，视图无需同步主数据，");
                        }
                    }
                    msg.append("\r\n");
                }
                msg.append("************************数据同步进度************************");
                logger.info(msg.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void printProgress(ArrayList<SubTask> subTasks) {
        if (subTasks != null) {
            for (SubTask subTask : subTasks) {
                ProgressUtil.put(subTask.getSourceDB() + "." + subTask.getSourceTB(), subTask);
            }
            ProgressUtil.printProgress();
        }
    }

    /**
     * @param fileName 结果输出文件名
     * @param str      主任务运行统计信息
     */
    public static void writeResult(String fileName, String str) {
        if (items != null) {
            FileOutputStream fos = null;
            BufferedOutputStream bos = null;
            try {
                //  指定默认值
                String fileDir = "/tmp/hive_sync/logs/result/";
                //  读取环境变量
                if (System.getenv("HIVE_SYNC_HOME") != null) {
                    fileDir = System.getenv("HIVE_SYNC_HOME");
                    if (fileDir.endsWith("/")) {
                        fileDir = fileDir.substring(0, fileDir.lastIndexOf("/"));
                    }
                    fileDir = fileDir + "/logs/result/";
                }
                fileName = fileDir + fileName;
                StringBuilder msg = new StringBuilder(str).append("\r\n");
                Set<String> keys = items.keySet();
                SubTask task = null;
                String metadataSyncStatus = null;
                String maindataSyncStatus = null;
                for (String key : keys) {
                    if (items.get(key) == null) {
                        msg.append("数据表").append(key).append("元数据未发生变化，本次数据同步跳过\r\n");
                    } else {
                        task = items.get(key);
                        metadataSyncStatus = task.getMetadataSyncStatus();
                        maindataSyncStatus = task.getMaindataSyncStatus();
                        msg.append("数据表").append(key);
                        if (Constant.TASK_STATUS_WAIT.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_WAIT.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("还未开始同步，");
                        } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_WAIT.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，主数据还未同步，");
                        } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_RUNNING.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，主数据正在同步，");
                        } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，主数据同步成功，");
                        } else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_FAIL.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，主数据同步失败，");
                        } else if (Constant.TASK_STATUS_FAIL.equalsIgnoreCase(metadataSyncStatus) && Constant.TASK_STATUS_WAIT.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步失败，主数据还未同步，");
                        }  else if (Constant.TASK_STATUS_FAIL.equalsIgnoreCase(metadataSyncStatus) && Constant.VIEW_MAINDATA_NDO.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步失败，视图无需同步主数据，");
                        }else if (Constant.TASK_STATUS_SUCCESS.equalsIgnoreCase(metadataSyncStatus) && Constant.VIEW_MAINDATA_NDO.equalsIgnoreCase(maindataSyncStatus)) {
                            msg.append("元数据同步成功，视图无需同步主数据，");
                        }
                        msg.append(DateUtil.getIntervalFormat(task.getCreateTime(), task.getUpdateTime())).append("\r\n");
                    }
                }

                //  判断结果输出目录和输出文件是否存在
                File file = new File(fileName);
                if (!file.exists()) {
                    if (!file.getParentFile().exists()) {
                        file.getParentFile().mkdirs();
                    }
                    System.out.println("++++++++++++++++++++++++++++==============================+++++++++++++++++++++++++++");
                    System.out.println(file.getPath());
                    file.createNewFile();
                }
                fos = new FileOutputStream(file);
                bos = new BufferedOutputStream(fos);
                bos.write(msg.toString().getBytes());

                logger.info("运行结果明细信息请查看" + fileName);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (bos != null) {
                        bos.close();
                    }
                    if (fos != null) {
                        fos.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
