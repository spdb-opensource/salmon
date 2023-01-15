package com.spdb.hive.sync.util.property;

import com.spdb.hive.sync.util.hive.HMSCUtil;
import com.spdb.hive.sync.util.hive.SDUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URLDecoder;
import java.util.Properties;
import java.util.Set;
import com.spdb.hive.sync.util.LogUtil;
import static com.spdb.hive.sync.util.confPath.ConfigPath.getConfPath;

/**
 * project：hive-sync
 * package：com.spdb.hive.sync.util.property
 * author：zhouxh
 * time：2021-04-19 9:45
 * description：
 */
public class PropUtil {

    // Logger和LoggerFactory导入的是org.slf4j包
    private final static Logger logger = LogUtil.getLogger();
    private static Properties properties = new Properties();

    private static InputStream input;
    private static InputStream is;
    /**
     * 静态代码块，加载配置文件cluster.properties
     */
    static {
        try {
            InputStream input = PropUtil.class.getClassLoader().getResourceAsStream("cluster.properties");
            properties.load(input);
            logger.info("默认配置文件加载成功：cluster.properties");
            printValues();
//            String bashPath = PropUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
//            bashPath = URLDecoder.decode(bashPath, "utf-8");
//            if (bashPath.endsWith(".jar")) {
//                bashPath = bashPath.substring(0, bashPath.lastIndexOf("/") + 1) + "../conf/cluster.properties";
//            }
            String bashPath=getConfPath()+"cluster.properties";
            InputStream is = new FileInputStream(bashPath);
            properties.load(is);
            logger.info("用户配置文件加载成功：cluster.properties");
            printValues();
        } catch (Exception e) {
            logger.warn("配置文件加载失败：cluster.properties " + e.getMessage());
            //  e.printStackTrace();
        }finally {
            try {
                input.close();
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 根据对应的key返回cluster.properties文件中value值
     *
     * @param key
     * @return 返回cluster.properties文件中value值
     */
    public static String getProValue(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            logger.info("不存在该键值对" + key);
        }
        return value;
    }

    /**
     * @param key   需要设置的key
     * @param value 需要设置的value
     */
    public static void setProValue(String key, String value) {
        properties.setProperty(key, value);
    }

    /**
     * 输出所有键值对
     */
    public static void printValues() {

        Set<Object> keys = properties.keySet();
        for (Object obj : keys) {
            logger.info(obj + " : " + properties.get(obj));
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println("hello world");
           /* String bashPath = PropUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            bashPath = URLDecoder.decode(bashPath, "utf-8");
            if (bashPath.endsWith(".jar")) {
                bashPath = bashPath.substring(0, bashPath.lastIndexOf("/") + 1) + "../conf/cluster.properties";
            }
            InputStream is = new FileInputStream(bashPath);
            properties.load(is);*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
