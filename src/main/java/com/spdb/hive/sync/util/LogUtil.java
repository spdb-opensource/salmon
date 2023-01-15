package com.spdb.hive.sync.util;

import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.FileInputStream;
import java.io.IOException;

public class LogUtil {

    public static Logger getLogger() {
        ConfigurationSource source;
        try {
            String logPath;
        if (System.getProperty("os.name").toLowerCase().contains("linux")) {
            logPath="/opt/plusSoft/salmon/conf/defconfig.xml";
        }else{
            logPath="D:\\zzzz\\xxxxc\\hive-sync123\\src\\main\\resources\\log4j2.xml";
        }
            source = new ConfigurationSource(new FileInputStream(logPath));
//            source = new ConfigurationSource(new FileInputStream("D:\\ziped\\projectsHadoopSync\\hive-sync123\\src\\main\\resources\\log4j2.xml"));
            Configurator.initialize(null, source);
            return LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
