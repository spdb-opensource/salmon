package com.spdb.hive.sync.concurrent;


import com.spdb.hive.sync.util.auth.Authkrb5;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;

public class authKrb5Thread implements Runnable {

    private final static Logger logger = LogManager.getLogger(authKrb5Thread.class);

    public authKrb5Thread() { }

    @Override
    public void run() {

        timeKrb5();

    }

    // 设定指定任务task在指定延迟delay后进行固定延迟peroid的执行
    // schedule(TimerTask task, long delay, long period)
    public static void timeKrb5() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                Authkrb5.authkrb5Default();
                System.out.println("执行认证bdr_admin");
                logger.info("认证用户线程执行用户一次");
            }
        }, 1000L*1800, 1000L*1800);
    }
}
