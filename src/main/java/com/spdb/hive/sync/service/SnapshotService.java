package com.spdb.hive.sync.service;

import com.spdb.hive.sync.entity.Snapshot;
import com.spdb.hive.sync.entity.SubTask;
import com.spdb.hive.sync.util.db.DBUtil;

import java.sql.*;
import java.util.ArrayList;

/**
 * project：hive-sync-1.2
 * package：com.spdb.hive.sync.service
 * author：zhouxh
 * time：2021-06-25 16:19
 * description：
 */
public class SnapshotService {

    public int insert(Snapshot snapshot) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "insert into snapshot(" +
                    "path" +
                    ",name" +
                    ",cluster" +
                    ",type" +
                    ",main_task_id" +
                    ") values(?,?,?,?,?) ";

            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, snapshot.getPath());
            ps.setString(2, snapshot.getName());
            ps.setString(3, snapshot.getCluster());
            ps.setString(4, snapshot.getType());
            ps.setInt(5, snapshot.getMainTaskId());
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

    /**
     * 根据主任务ID查询源集群快照
     *
     * @param mainTaskId 主任务ID
     * @return 源集群快照集合
     */
    public ArrayList<Snapshot> querySourceSnapshotByMainTaskId(int mainTaskId) {
        ArrayList<Snapshot> snapshots = new ArrayList<Snapshot>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "select * from snapshot where main_task_id=? " +
                    " and type='source'" +
                    " and delete_flag != 1";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, mainTaskId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Snapshot snapshot = new Snapshot();
                snapshot.setId(rs.getInt("id"));
                snapshot.setPath(rs.getString("path"));
                snapshot.setName(rs.getString("name"));
                snapshot.setCluster(rs.getString("cluster"));
                snapshot.setType(rs.getString("type"));
                snapshot.setMainTaskId(rs.getInt("main_task_id"));
                snapshot.setCreateTime(rs.getString("create_time"));
                snapshot.setUpdateTime(rs.getString("update_time"));
                snapshot.setDeleteFlag(rs.getInt("delete_flag"));
                snapshots.add(snapshot);
            }
            return snapshots;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs, ps, conn);
        }
        return null;
    }

    /**
     * 删除快照
     *
     * @param id 需要删除的快照数据id
     * @return
     */
    public int delete(int id) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "update snapshot set " +
                    "delete_flag=1" +
                    " where id=?";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, id);
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
