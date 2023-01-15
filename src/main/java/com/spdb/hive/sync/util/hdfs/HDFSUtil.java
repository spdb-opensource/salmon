package com.spdb.hive.sync.util.hdfs;

import com.spdb.hive.sync.entity.Snapshot;
import com.spdb.hive.sync.entity.SubTask;
import com.spdb.hive.sync.entity.SyncEntity;
import com.spdb.hive.sync.service.HMSCService;
import com.spdb.hive.sync.service.SnapshotService;
import com.spdb.hive.sync.util.auth.Authkrb5;
import com.spdb.hive.sync.util.date.DateUtil;
import com.spdb.hive.sync.util.hive.HMSCUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hive.metastore.MyHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import com.spdb.hive.sync.util.LogUtil;
/**
 * project：hive-sync-1.2
 * package：com.spdb.hive.sync.util.hdfs
 * author：zhouxh
 * time：2021-06-23 10:27
 * description：
 */
public class HDFSUtil {
    private final static Logger logger = LogUtil.getLogger();
    private static SnapshotService snapshotService = new SnapshotService();
    private static HMSCService hmscService = new HMSCService();
    //  存放源集群每个目录对应的目标集群目录
    //  private static HashMap<String, String> sourceDirToTargetDir = new HashMap<>();
    //  存放源集群每个hdfs目录路径和对应的快照目录路径
    private static HashMap<String, String> sourceDirToSnapDir = new HashMap<String, String>();

    /**
     * 创建快照
     *
     * @param mainTaskId    主任务ID
     * @param sourceCluster 源集群
     * @param targetCluster 目标集群
     * @param ses           同步实体集合
     * @return 本次数据同步的快照名称
     */
    public static String createSnapshot(int mainTaskId, String sourceCluster, String targetCluster, List<SyncEntity> ses) {
        try {
            //  解析出源集群和目标集群的hdfs目录列表
            HashMap<String, List<String>> map = getHDFSDirList(ses);
            String tmp = null;

            //  获得源集群可快照目录集合
            ArrayList<String> sourceSnapshottableDirs = getSnapshottableDirs(sourceCluster);
            //  获得源集群待同步hdfs目录
            List<String> sourceHDFSDirs = map.get(sourceCluster);
            //  存放源集群需要进行快照的目录，这些目录首先是一个可快照目录
            HashSet<String> sourceNeedSnapshotDirs = new HashSet<>();
            //  存放源集群不满足条件的hdfs目录，这些目录没有包含任何可快照目录，需要集群管理员手动将其设置为可快照目录
            ArrayList<String> sourceDontSatisfySnapshotDirs = new ArrayList<>();
            for (String hdfsDir : sourceHDFSDirs) {
                //  可快照目录
                tmp = isContains(hdfsDir, sourceSnapshottableDirs);
                if (tmp != null) {
                    sourceNeedSnapshotDirs.add(tmp);
                    //  存储每个hdfs表级目录对应的可快照目录
                    sourceDirToSnapDir.put(hdfsDir, tmp);
                } else {
                    //  当前hdfs目录不包含任何可快照目录
                    sourceDontSatisfySnapshotDirs.add(hdfsDir);
                }
            }
            logger.info("源集群" + sourceCluster + "当前需要进行快照的目录为：" + sourceNeedSnapshotDirs);
            logger.info("源集群" + sourceCluster + "每个目录对应的快照目录为：" + sourceDirToSnapDir);

            //  获得目标集群可快照目录集合
            ArrayList<String> targetSnapshottableDirs = getSnapshottableDirs(targetCluster);
            //  获得目标集群待同步hdfs目录
            List<String> targetHDFSDirs = map.get(targetCluster);
            //  存放目标集群需要进行快照的目录，这些目录首先是一个可快照目录
            HashSet<String> targetNeedSnapshotDirs = new HashSet<>();
            //  存放目标集群不满足条件的hdfs目录，这些目录没有包含任何可快照目录，需要集群管理员手动将其设置为可快照目录
            ArrayList<String> targetDontSatisfySnapshotDirs = new ArrayList<>();
            for (String hdfsDir : targetHDFSDirs) {
                tmp = isContains(hdfsDir, targetSnapshottableDirs);
                if (tmp != null) {
                    targetNeedSnapshotDirs.add(tmp);
                } else {
                    //  当前hdfs目录不包含任何可快照目录
                    targetDontSatisfySnapshotDirs.add(hdfsDir);
                }
            }
            logger.info("目标集群" + targetCluster + "当前需要进行快照的目录为：" + targetNeedSnapshotDirs);

            if (sourceDontSatisfySnapshotDirs.size() > 0) {
                logger.error("源集群" + sourceCluster + "存在不可快照的目录" + sourceDontSatisfySnapshotDirs +
                        "，建议通知管理员将其设置为可快照目录或重新运行数据同步脚本并追加-snapshot false参数，以不基于快照的方式进行数据同步（谨慎使用-snapshot false）");
            }

            if (targetDontSatisfySnapshotDirs.size() > 0) {
                logger.error("目标集群" + targetCluster + "存在不可快照的目录" + targetDontSatisfySnapshotDirs +
                        "，建议通知管理员将其设置为可快照目录或重新运行数据同步脚本并追加-snapshot false参数，以不基于快照的方式进行数据同步（谨慎使用-snapshot false）");
            }

            if (sourceDontSatisfySnapshotDirs.size() > 0 || targetDontSatisfySnapshotDirs.size() > 0) {
                return null;
            }
            long timestamp = System.currentTimeMillis();
            String date = DateUtil.getDate(timestamp);
            //  快照名称
            String snapshotName = "hive-sync-" + mainTaskId + "-" + date.substring(0, 10) + "-" + timestamp;
            //  源集群创建快照
            if (!create(mainTaskId, sourceCluster, "source", snapshotName, sourceNeedSnapshotDirs)) {
                return null;
            }
            //  目标集群创建快照
            if (!create(mainTaskId, targetCluster, "target", snapshotName, targetNeedSnapshotDirs)) {
                return null;
            }

            //  转换源集群每张表的hdfs location为快照location
            Set<String> keys = sourceDirToSnapDir.keySet();
            String value = null;
            for (String key : keys) {
                value = sourceDirToSnapDir.get(key);
                sourceDirToSnapDir.put(key, key.replace(value, value + "/.snapshot/" + snapshotName));
            }
            logger.info("源集群" + sourceCluster + "每个目录转换后的快照目录为：" + sourceDirToSnapDir);
            return snapshotName;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 创建hdfs快照
     *
     * @param cluster                集群名称
     * @param type                   类型
     * @param snapshotName           快照名称
     * @param sourceNeedSnapshotDirs 需要创建快照的目录集合
     */
    public static boolean create(int mainTaskId, String cluster, String type, String snapshotName, HashSet<String> sourceNeedSnapshotDirs) {
        DistributedFileSystem dfs = null;
        try {
            dfs = getDFS(cluster);
            for (String path : sourceNeedSnapshotDirs) {
                try {
                    //  创建快照
                    dfs.createSnapshot(new Path(path), snapshotName);
                    logger.info(cluster + "集群" + path + "目录快照创建成功，快照名：" + snapshotName);
                    //  数据入库
                    snapshotService.insert(new Snapshot(
                            path,
                            snapshotName,
                            cluster,
                            type,
                            mainTaskId
                    ));
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error(cluster + "集群" + path + "目录快照创建失败，快照名：" + snapshotName);
                    return false;
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            close(dfs);
        }
    }

    /**
     * 清理源集群快照
     *
     * @param mainTaskId 主任务ID
     */
    public static void deleteSnapshots(int mainTaskId) {
        //  根据主任务ID查询源集群快照
        ArrayList<Snapshot> snapshots = snapshotService.querySourceSnapshotByMainTaskId(mainTaskId);
        //  遍历
        if (snapshots != null && snapshots.size() > 0) {
            String cluster = snapshots.get(0).getCluster();
            logger.info("开始清理" + cluster + "集群本次数据同步创建的快照，待清理的快照有" + snapshots.size() + "个");
            DistributedFileSystem dfs = null;
            try {
                dfs = getDFS(cluster);
                for (Snapshot snapshot : snapshots) {
                    //  删除快照
                    dfs.deleteSnapshot(new Path(snapshot.getPath()), snapshot.getName());
                    //  更新数据库
                    snapshotService.delete(snapshot.getId());
                    logger.info(cluster + "集群" + snapshot.getPath() + "路径的" + snapshot.getName() + "快照删除成功");
                }
                //logger.info(cluster + "集群本次数据同步创建的快照全部清理完成");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                close(dfs);
            }
        } else {
            logger.info("本次没有待清理的快照，主任务ID：" + mainTaskId);
        }
    }

    /**
     * @param ses 同步实体集合
     * @return
     */
    public static HashMap<String, List<String>> getHDFSDirList(List<SyncEntity> ses) {
        if (ses != null && ses.size() > 0) {
            HashMap<String, List<String>> map = new HashMap<String, List<String>>();
            //  获得源集群名称
            String sourceCluster = ses.get(0).getSourceCluster();
            //  获得目标集群名称
            String targetCluster = ses.get(0).getTargetCluster();
            MyHiveMetaStoreClient shmsc = null;
            try {
                //  获得源集群的HMSC对象
                shmsc = HMSCUtil.getHMSC(sourceCluster + ".hive.conf");
                Table table = null;
                String tableType = null;
                String sLocation = null;
                String tLocation = null;
                //  存放源表所有location
                ArrayList<String> sourceDirs = new ArrayList<String>();
                //  存放目标表所有location
                ArrayList<String> targetDirs = new ArrayList<String>();
                //  遍历ses
                for (SyncEntity se : ses) {
                    //  获得源表
                    table = shmsc.getTable(se.getSourceDB(), se.getSourceTB());
                    tableType = table.getTableType();
                    sLocation = table.getSd().getLocation();
                    sourceDirs.add(sLocation);
                    if (tableType.equalsIgnoreCase("MANAGED_TABLE") || tableType.equalsIgnoreCase("EXTERNAL_TABLE")) {
                        tLocation = hmscService.replaceLocation(se, sLocation, tableType);
                        targetDirs.add(tLocation);
                        //sourceDirToTargetDir.put(sLocation, tLocation);
                    }
                }
                map.put(sourceCluster, sourceDirs);
                map.put(targetCluster, targetDirs);
                return map;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            } finally {
                HMSCUtil.close(shmsc);
            }
        } else {
            logger.warn("ses为空");
            System.exit(0);
        }

        return null;
    }

    /**
     * @param dfs  DistributedFileSystem客户端
     * @param path 指定hdfs路径
     * @return 返回指定hdfs路径下所有文件绝对路径
     */
    public static ArrayList<String> getHDFSFileList(DistributedFileSystem dfs, String path) {
        try {
            if (dfs.exists(new Path(path))) {
                ArrayList<String> files = new ArrayList<>();
                RemoteIterator<LocatedFileStatus> listFiles = dfs.listFiles(new Path(path), true);
                while (listFiles.hasNext()) {
                    files.add(listFiles.next().getPath().toString());
                }
                return files;
            } else {
                logger.warn("不存在路径" + path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param sourceCluster 源集群
     * @param targetCluster 目标集群
     * @param snapshotName  快照名称
     * @param subTasks      元数据和主数据都同步成功的子任务集合
     * @return 返回文件一致性检查的结果
     */
    public static boolean consistencyCheck(String sourceCluster, String targetCluster, String snapshotName, ArrayList<SubTask> subTasks) {
        logger.info("开始进行数据文件一致性比对");
        if (subTasks == null || subTasks.size() < 1) return false;
        //  存储源集群每个文件路径所对应的目标集群文件路径，源路径是包含快照级别目录的，而目标集群这边是不包含快照级别目录的
        HashMap<String, String> sourceFileToTargetFile = new HashMap<>();
        MyHiveMetaStoreClient shmsc = null;
        //  源集群的DistributedFileSystem客户端
        DistributedFileSystem sdfs = null;
        //  目标集群的DistributedFileSystem客户端
        DistributedFileSystem tdfs = null;
        String path = null;
        //  存储本次所有同步成功的表的文件路径（是带快照目录的路径）
        ArrayList<String> files = null;
        try {
            sdfs = getDFS(sourceCluster);
            tdfs = getDFS(targetCluster);
            //  获得源集群的HMSC对象
            shmsc = HMSCUtil.getHMSC(sourceCluster + ".hive.conf");
            Table table = null;
            String tableType = null;
            String tLocation = null;
            String prefix = null;
            String suffix = null;
            //  遍历每个执行成功的子任务
            for (SubTask subTask : subTasks) {
                //  拿到源路径，这里的路径实际上包含快照目录的hdfs路径
                path = subTask.getMaindataSyncSourcePath();
                //  返回指定hdfs路径下所有文件绝对路径
                files = getHDFSFileList(sdfs, path);
                if (files != null && files.size() > 0) {
                    //  遍历并转换当前子任务对应的表目录下所有文件路径
                    for (String file : files) {
                        //  根据表的内外部属性进行路径转换
                        table = shmsc.getTable(subTask.getSourceDB(), subTask.getSourceTB());
                        tableType = table.getTableType();
                        //  这里不能直接使用file，因为file是包含了文件的完整路径，在hmscService.replaceLocation方法中都是根据表级别和分区级别进行路径转换
                        //  这里先将file里面的文件名路径截取下来保留，最后再拼接上去，这么做是因为文件名中有可能包含表名，如果全部替换为目标表名的话会导致源集群这边的文件到目标集群找不到
                        suffix = file.substring(file.lastIndexOf("/"), file.length());
                        prefix = file.substring(0, file.lastIndexOf("/"));

                        //  file是包含了快照路径的文件路径，如果直接用这个路径去调用hmscService.replaceLocation方法的话是很难判断具体把哪个路径替换为/user/hive/warehouse的
                        //  因为从CDP7同步到CDH6或者CDH5时快照目录不同的话需要替换的目录就不同
                        //  这里先将file路径根据sourceDirToSnapDir中存储的路径进行转换，sourceDirToSnapDir中的key为源集群的表级别落地目录，value为源集群的快照目录
                        //  根据file的prefix找到其对应的落地目录，然后用这个落地目录去调用hmscService.replaceLocation方法，转换成目标集群对应目录，再拼接suffix
                        prefix = getFileLocation(prefix);

                        //  这里是将源路径转换成目标集群的文件路径
                        tLocation = hmscService.replaceLocation(new SyncEntity(
                                        sourceCluster,
                                        subTask.getSourceDB(),
                                        subTask.getSourceTB(),
                                        targetCluster,
                                        subTask.getTargetDB(),
                                        subTask.getTargetTB()),
                                prefix,
                                tableType);
                        //  如果快照名称不为空说明走的是基于快照的数据同步，为空说明是基于落地数据的数据同步，将tLocation中的快照路径去除
                        /*if (snapshotName != null) {
                            tLocation = tLocation.replace("/.snapshot/" + snapshotName, "");
                        }*/
                        sourceFileToTargetFile.put(file, tLocation + suffix);
                    }
                } else {
                    logger.warn(sourceCluster + "的" + path + "目录为空");
                }
            }
            //  进行文件一致性校验
            Set<String> keys = sourceFileToTargetFile.keySet();
            //  存储checksum值不一致的文件
            HashMap<String, String> notConsistencyFiles = new HashMap<>();
            String t = null;
            for (String s : keys) {
                t = sourceFileToTargetFile.get(s);
                //  比较源集群文件和目标集群文件的checksum值是否一致
                if (!compareChecksum(sdfs, tdfs, s, t)) {
                    logger.error(sourceCluster + "的" + s + "文件同步到" + targetCluster + "集群后和" + t + "文件的checksum值不一致");
                    notConsistencyFiles.put(s, t);
                } else {
                    logger.debug(sourceCluster + "的" + s + "文件同步到" + targetCluster + "集群后和" + t + "文件的checksum值一致");
                }
            }
            if (notConsistencyFiles.size() > 0) {
                //logger.error("数据文件一致性比对发现有不一致的数据：" + notConsistencyFiles);
                return false;
            }
            //logger.info("数据文件一致性比对完全一致");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            HMSCUtil.close(shmsc);
            close(sdfs);
            close(tdfs);
        }
    }

    /**
     * 比较源集群文件和目标集群文件的checksum值是否一致
     *
     * @param sdfs   源集群DistributedFileSystem
     * @param tdfs   目标集群DistributedFileSystem
     * @param source 源文件
     * @param target 目标文件
     * @return 如果源文件和目标文件checksum值相等返回true，否则返回false
     */
    public static boolean compareChecksum(DistributedFileSystem sdfs, DistributedFileSystem tdfs, String source, String target) {
        try {
            //  得到源文件的checksum值
            FileChecksum sfcs = sdfs.getFileChecksum(new Path(source));
            if (!tdfs.exists(new Path(target))) {
                logger.error("目标集群不存在该文件" + target);
                return false;
            }
            //  得到目标文件的checksum值
            FileChecksum tfcs = tdfs.getFileChecksum(new Path(target));
            //  比较checksum值是否相等
            if (sfcs.toString().equalsIgnoreCase(tfcs.toString()))
                return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * @param cluster 集群名称
     * @return 获得对应集群的DistributedFileSystem
     */
    public static DistributedFileSystem getDFS(String cluster) {
        try {
            Configuration sc = new Configuration();
            sc.addResource(cluster + "/core-site.xml");
            sc.addResource(cluster + "/hdfs-site.xml");
            DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(sc);
            return dfs;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param dfs 对应集群的DistributedFileSystem对象
     * @return 获得集群所有可快照目录
     */
    public static ArrayList<String> getSnapshottableDirs(DistributedFileSystem dfs) {
        try {
            ArrayList<String> dirs = new ArrayList<>();
            //  获得集群所有可快照目录
            SnapshottableDirectoryStatus[] ssds = dfs.getSnapshottableDirListing();
            for (SnapshottableDirectoryStatus sds : ssds) {
                dirs.add(sds.getFullPath().toString());
            }
            return dirs;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param cluster 集群名称
     * @return 获得集群所有可快照目录
     */
    public static ArrayList<String> getSnapshottableDirs(String cluster) {
        logger.info("当前集群为" + cluster);
        DistributedFileSystem dfs = null;
        try {
            ArrayList<String> dirs = new ArrayList<>();
            //  获得对应集群的DistributedFileSystem
            dfs = getDFS(cluster);
            if (dfs != null) {
                //  获得集群所有可快照目录
                SnapshottableDirectoryStatus[] ssds = dfs.getSnapshottableDirListing();
                if (ssds != null && ssds.length > 0) {
                    for (SnapshottableDirectoryStatus sds : ssds) {
                        dirs.add(sds.getFullPath().toString());
                    }
                }
                logger.info("当前集群" + cluster + "可快照目录为" + dirs);
                return dirs;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(dfs);
        }
        return null;
    }

    public static void close(DistributedFileSystem dfs) {
        try {
            if (dfs != null) {
                dfs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断给定的hdfs路径中是否有可快照目录
     *
     * @param hdfsDir           hdfs路径
     * @param snapshottableDirs 所有可快照目录集合
     * @return 如果包含则返回可快照目录，如果不包含则返回null
     */
    public static String isContains(String hdfsDir, ArrayList<String> snapshottableDirs) {
        if (snapshottableDirs != null && snapshottableDirs.size() > 0) {
            for (String snapshottableDir : snapshottableDirs) {
                if (hdfsDir.contains(snapshottableDir)) {
                    return snapshottableDir;
                }
            }
        }
        return null;
    }

    /**
     * 根据表级别的落地路径得到对应的快照路径
     *
     * @param key 表级别的落地路径
     * @return 对应的快照路径
     */
    public static String getSnapshotLocation(String key) {
        return sourceDirToSnapDir.get(key);
    }

    /**
     * @param filePath 源集群包括快照路径的文件绝对路径
     * @return 返回源集群中不包含快照的文件落地路径
     */
    public static String getFileLocation(String filePath) {
        Set<String> keys = sourceDirToSnapDir.keySet();
        String value = null;
        for (String key : keys) {
            value = sourceDirToSnapDir.get(key);
            if (filePath.contains(value)) {
                return filePath.replace(value, key);
            }
        }
        return filePath;
    }

    public static void main(String[] args) throws IOException {
        Authkrb5.authkrb5Default();
        DistributedFileSystem dfs = getDFS("dongcha");

        ArrayList<String> paths = new ArrayList<>();
        paths.add("/user/hive/warehouse/bdpd_rpt.db/odsx_rpt_dm_univ_emp_org_pcrm_h_if/pt_dt=2019-12-10/000000_0");
        paths.add("/user/hive/warehouse/bdpd_rpt.db/odsx_rpt_bf_rt_lo_ctr_agrm_if/pt_dt=2018-12-31/000015_0");
        paths.add("/user/hive/warehouse/stag.db/yc_lal/000000_0");
        paths.add("/user/hive/warehouse/bdpd_rpt.db/odsx_rpt_bf_rt_lo_ctr_agrm_if_01/pt_dt=2018-12-31/com_rpt_bf_rt_lo_ctr_agrm_if_20181231.txt");
        paths.add("/user/hive/warehouse/stag.db/odsx_pdl_c01_s_indiv_cust_prod_hold_txt/pt_dt=2018-12-31/com_pdl_c01_s_indiv_cust_prod_hold_20181231.txt");
        paths.add("/user/hive/warehouse/graph_01.db/p_test_allot_fact/allot_fact.txt");
        paths.add("/user/hive/warehouse/bdpd_dla.db/l_indvcst_ff_fnc_pd_td_kylin/pt_dt=20190930/000004_0");
        paths.add("/user/hive/warehouse/bdpd_dla.db/l_indvcst_ff_fnc_pd_td_kylin/pt_dt=20190930/000000_0");

        for (String path : paths) {
            long start = System.currentTimeMillis();
            FileChecksum fcs = dfs.getFileChecksum(new Path(path));
            System.out.println(path + " 耗时：" + (System.currentTimeMillis() - start));
        }

    }
}
