package com.spdb.hive.sync.base;

/**
 * @author xujie
 * @ClassName BaseException
 * @date 2020/4/27 9:27
 */
public class BaseException extends RuntimeException {


    /**
     * 构造方法
     *
     * @param msg : 异常信息
     */
    public BaseException(String msg) {
        super(msg);
    }

    /**
     * 构造方法
     *
     * @param msg : 异常信息
     * @param ex  : 异常
     */
    public BaseException(String msg, Exception ex) {
        super(msg, ex);
    }
}
