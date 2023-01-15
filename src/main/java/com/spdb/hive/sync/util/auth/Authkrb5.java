package com.spdb.hive.sync.util.auth;

import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import com.spdb.hive.sync.util.LogUtil;
/**
 * project：hive-test
 * package：com.spdb.hive.replication.util.auth
 * author：zhouxh
 * time：2021-03-29 14:09
 * description：进行源集群和目标集群的身份认证工具类
 */
public class Authkrb5 {

    // Logger和LoggerFactory导入的是org.slf4j包
    private final static Logger logger = LogUtil.getLogger();
    /**
     * 进行kerberos身份认证
     *
     * @param krb5ConfPath kr5.conf配置文件路径
     * @param krb5User     kerberos用户名和认证的域
     * @param ktPath       krb5User用户生成的key.tab配置文件路径
     */
    public static void authkrb5(String krb5ConfPath, String krb5User, String ktPath) {
        try {
            System.setProperty("java.security.krb5.conf", krb5ConfPath);

            Configuration conf = new Configuration();

            conf.setBoolean("hadoop.security,authorization", true);

            conf.set("hadoop.security.authentication", "kerberos");

            UserGroupInformation.setConfiguration(conf);

            UserGroupInformation.loginUserFromKeytab(krb5User, ktPath);

            logger.info("Kerberos 认证成功");
        } catch (IOException e) {
            logger.error("Kerberos 认证失败，程序退出");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void authkrb5Default() {
        if (System.getProperty("os.name").toLowerCase().contains("linux")) {
            String krbPath = PropUtil.getProValue("krb.path");
            String krbUser = PropUtil.getProValue("krb.user");
            authkrb5(krbPath + "/krb5.conf",
                    krbUser,
                    krbPath + "/admin.keytab");
        } else {
            authkrb5("D:\\projectsNew\\hadoop_data_sync121\\src\\main\\resources\\krb\\krb5.conf",
                    "admin@MCIPT.COM",
                    "D:\\projectsNew\\hadoop_data_sync121\\src\\main\\resources\\krb\\admin.keytab");
        }

    }

    public static void main(String[] args) {
        authkrb5Default();
    }
}
