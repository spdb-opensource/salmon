package com.spdb.hive.sync.util.confPath;

import com.spdb.hive.sync.util.property.PropUtil;

public class ConfigPath {

    public static String getConfPath(){
        String bashPath = PropUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        return bashPath.substring(0, bashPath.lastIndexOf("/")-3)+"conf/";
    }

    public static void main(String[] args) {
        getConfPath();
    }
}
