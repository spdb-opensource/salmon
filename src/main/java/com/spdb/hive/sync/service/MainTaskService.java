package com.spdb.hive.sync.service;

import com.spdb.hive.sync.entity.MainTask;
import com.spdb.hive.sync.util.date.DateUtil;
import com.spdb.hive.sync.util.db.DBUtil;

import java.sql.*;

/**
 * project：hive-sync
 * package：com.spdb.hive.sync.service
 * author：zhouxh
 * time：2021-04-16 9:39
 * description：
 */
public class MainTaskService {

    public int insert(MainTask mainTask) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "insert into main_task(" +
                    "main_task_args" +
                    ",source_cluster"+
                    ",target_cluster"+
                    ",main_task_status" +
                    ",main_task_result" +
                    ") values(?,?,?,?,?) ";

            conn = DBUtil.getConnection();

            ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, mainTask.getMainTaskArgs());
            ps.setString(2,mainTask.getSourceCluster());
            ps.setString(3,mainTask.getTargetCluster());
            ps.setString(4, mainTask.getMainTaskStatus());
            ps.setString(5, mainTask.getMainTaskResult());
            ps.execute();
            //  返回自增主键
            rs = ps.getGeneratedKeys();
            int id = -1;
            if (rs.next()) {
                //  获得自增主键
                id = rs.getInt(1);
            }

            return id;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs, ps, conn);
        }
        return -1;
    }

    public MainTask queryById(int id) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "select * from main_task where id=?";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, id);
            rs = ps.executeQuery();

            MainTask mainTask = new MainTask();
            while (rs.next()) {
                mainTask.setId(rs.getInt("id"));
                mainTask.setMainTaskArgs(rs.getString("main_task_args"));
                mainTask.setSourceCluster(rs.getString("source_cluster"));
                mainTask.setTargetCluster(rs.getString("target_cluster"));
                mainTask.setMainTaskStatus(rs.getString("main_task_status"));
                mainTask.setMainTaskResult(rs.getString("main_task_result"));
                mainTask.setStartTime(rs.getString("start_time"));
                mainTask.setStopTime(rs.getString("stop_time"));
                mainTask.setCreateTime(rs.getString("create_time"));
                mainTask.setUpdateTime(rs.getString("update_time"));
                return mainTask;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs, ps, conn);
        }
        return null;
    }

    public int update(MainTask mainTask) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "update main_task set " +
                    "main_task_status=?" +
                    ",main_task_result=?" +
                    ",stop_time=?" +
                    "where id=?";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setString(1, mainTask.getMainTaskStatus());
            ps.setString(2, mainTask.getMainTaskResult());
            ps.setString(3, mainTask.getStopTime());
            ps.setInt(4, mainTask.getId());
            int i = ps.executeUpdate();
            return i;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs, ps, conn);
        }
        return -1;
    }

}
