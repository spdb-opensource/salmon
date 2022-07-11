package com.spdb.hive.sync.concurrent;

import com.spdb.hive.sync.entity.SyncEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * project：hive-sync
 * package：com.spdb.hive.sync.concurrent
 * author：zhouxh
 * time：2021-04-19 9:56
 * description：
 */
public class TableQueue {

    private static CopyOnWriteArrayList<SyncEntity> ses = new CopyOnWriteArrayList<SyncEntity>();

    public static void addSE(SyncEntity se) {
        ses.add(se);
    }

    public static void addSE(List<SyncEntity> items) {
        ses.addAll(items);
    }

    public static synchronized SyncEntity getSE() {
        try {
            if (ses.isEmpty()) {
                return null;
            } else {
                return ses.remove(0);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    }
}
