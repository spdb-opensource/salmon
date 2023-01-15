package com.spdb.hive.sync.distcp;

import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.util.LogUtil;
import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

import static com.spdb.hive.sync.util.confPath.ConfigPath.getConfPath;

public class HdfsFileDistcp {
    private final static Logger logger = LogUtil.getLogger();


    /**
     * @描述： 同步文件
     * @param sourceCluster :原集群名称  bdpdiylnameservice1
     * @param targetCluster :目标名称  bdpdsnameservice1
     * @param sourceFilePath :源文件路径  /user/hive/warehouse/zxh_sync_data_bak.db/order_info
     * @param targetFilePath :目标路径   /user/hive/warehouse/zxh_sync_data_bak.db/ordeo
     * @author zhouzl3
     * @date 2022/3/3 0:16
     * @return java.lang.String
     */
    public String syncFileData(String sourceCluster ,String targetCluster ,String sourceFilePath ,String targetFilePath) {

            Job job = null;
        try {
            String sourcePath = "hdfs://"+sourceCluster+sourceFilePath;
            String targetPath = "hdfs://"+targetCluster+targetFilePath;

            logger.info("开始同步主数据：" + sourcePath + "\t" + targetPath);

            Configuration conf = new Configuration();
            //  distcp nameservices配置文件路径，包含所有集群的nameservice解析
            conf.addResource(getConfPath()+PropUtil.getProValue(Constant.DISTCP_NAMESERVICES_CONF));
            //  distcp所提交的yarn集群配置文件路径，即yarn-site.xml配置文件路径
            conf.addResource(getConfPath()+PropUtil.getProValue(Constant.DISTCP_YARN_CONF));
            conf.set("mapreduce.job.hdfs-servers.token-renewal.exclude", sourceCluster+","+targetCluster);
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
            //delete参数
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

            //  封装option
            DistCpOptions options = builder.build();

            logger.info("当前生效的distcp参数：" + options.toString());
            System.out.println("当前生效的distcp参数：" + options.toString());
            System.out.println("======================当前生效的distcp配置：==================" + conf.toString());
            //  创建distcp

            DistCp distCp = null;
            try {
                distCp = new DistCp(conf, options);
                job = distCp.createAndSubmitJob();
                logger.info("作业已经提交，" + job.getJobID() + "，等待作业运行结束");
                distCp.waitForJobCompletion(job);
            } catch (Exception ex) {
                ex.printStackTrace();
                logger.error(ex.getMessage());
            }


            //  更新子任务对象的maindata_sync_status、maindata_sync_job_status、maindata_sync_stop、update_time
            String result = job.getJobState().name();

            logger.info("作业运行结束，" + job.getJobID() + "，结果状态：" + job.getJobState());
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            //  更新子任务对象的maindata_sync_status、
            return e.getMessage();
        }

    }


    public static void main(String[] args) {


    }


}
