package com.spdb.hive.sync.distcp;

import com.spdb.hive.sync.util.auth.Authkrb5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 * project：hive-sync-1.1
 * package：com.spdb.hive.sync.service
 * author：zhouxh
 * time：2021-05-25 17:12
 * description：
 */
public class TestDistCP {
    private final static Logger logger = LogManager.getLogger(TestDistCP.class);

    public static void main(String[] args) {
        Authkrb5.authkrb5Default();
        Job job = null;
        try {
            String sourcePath = "hdfs://bdpdiylnameservice1/warehouse/tablespace/managed/hive/zxh_sync_data.db/order_info";
            String targetPath = "hdfs://bdpdsnameservice1/user/hive/warehouse/zxh_sync_data_bak.db/order_info";

            logger.info("开始同步：" + sourcePath + "\t" + targetPath);

            Configuration conf = new Configuration();
            conf.addResource("distcp/hdfs-site.xml");
            conf.addResource("distcp/yarn-site.xml");
            conf.set("mapreduce.job.hdfs-servers.token-renewal.exclude", getNameservices(sourcePath, targetPath));

            //设置ssl验证
//            System.setProperty("javax.net.ssl.trustStore", "D:\\ziped\\projectsNew\\hadoop_data_sync121\\src\\main\\resources\\krb\\beeline_util01.jks");


            conf.get(MRConfig.FRAMEWORK_NAME,MRConfig.LOCAL_FRAMEWORK_NAME);

            conf.set("mapreduce.framework.name","yarn");
            conf.set("fs.defaultFS","hdfs://bdpdiylnameservice1");

            ArrayList<Path> sourcePaths = new ArrayList<Path>();

            sourcePaths.add(new Path(sourcePath));
            Path des = new Path(targetPath);

            DistCpOptions.Builder builder = new DistCpOptions.Builder(sourcePaths, des);

            builder
                    //  等同于命令行中的skipCRC=false，进行CRC文件内容校验，防止文件名和大小一致数据内容不一致时没有进行比对
                    .withCRC(true)
                    //  等同于-update
                    //.withSyncFolder(true)
                    //  等同于-overwrite
                    .withOverwrite(true);

            DistCpOptions options = builder.build();

            DistCp distCp = new DistCp(conf, options);
            job = distCp.createAndSubmitJob();
            distCp.waitForJobCompletion(job);
            //  更新子任务对象的maindata_sync_status、maindata_sync_job_status、maindata_sync_stop、update_time
            String result = job.getJobState().name();

            logger.info("执行结束：" + job.getJobID().toString() + "\t" + result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static String getNameservices(String source, String target) {
        String sourceUri = source.substring(source.indexOf("hdfs://") + 7);
        String sourceNameservice = sourceUri.substring(0, sourceUri.indexOf("/"));
        String targetUri = target.substring(target.indexOf("hdfs://") + 7);
        String targetNameservice = targetUri.substring(0, targetUri.indexOf("/"));
        return sourceNameservice + "," + targetNameservice;
    }
}
