package com.spdb.hive.sync.service;

import com.spdb.hive.sync.entity.MainTask;
import com.spdb.hive.sync.entity.SubTask;
import com.spdb.hive.sync.util.date.DateUtil;
import com.spdb.hive.sync.util.db.DBUtil;

import java.sql.*;
import java.util.ArrayList;

/**
 * project：hive-sync
 * package：com.spdb.hive.sync.service
 * author：zhouxh
 * time：2021-04-16 13:23
 * description：
 */
public class SubTaskService {

    public int insert(SubTask subTask) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "insert into sub_task(" +
                    "source_cluster" +
                    ",source_db" +
                    ",source_tb" +
                    ",target_cluster" +
                    ",target_db" +
                    ",target_tb" +
                    ",metadata_sync_status" +
                    ",metadata_sync_retry" +
                    ",maindata_sync_status" +
                    ",maindata_sync_source_path" +
                    ",maindata_sync_target_path" +
                    ",maindata_sync_args" +
                    ",maindata_sync_job_id" +
                    ",maindata_sync_job_status" +
                    ",maindata_sync_retry" +
                    ",maindata_sync_start" +
                    ",maindata_sync_stop" +
                    ",main_task_id" +
                    ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, subTask.getSourceCluster());
            ps.setString(2, subTask.getSourceDB());
            ps.setString(3, subTask.getSourceTB());
            ps.setString(4, subTask.getTargetCluster());
            ps.setString(5, subTask.getTargetDB());
            ps.setString(6, subTask.getTargetTB());
            ps.setString(7, subTask.getMetadataSyncStatus());
            ps.setInt(8, subTask.getMetadataSyncRetry());
            ps.setString(9, subTask.getMaindataSyncStatus());
            ps.setString(10, subTask.getMaindataSyncSourcePath());
            ps.setString(11, subTask.getMaindataSyncTargetPath());
            ps.setString(12, subTask.getMaindataSyncArgs());
            ps.setString(13, subTask.getMaindataSyncJobId());
            ps.setString(14, subTask.getMaindataSyncJobStatus());
            ps.setInt(15, subTask.getMaindataSyncRetry());
            ps.setString(16, subTask.getMaindataSyncStart());
            ps.setString(17, subTask.getMaindataSyncStop());
            ps.setInt(18, subTask.getMainTaskId());

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

    public SubTask queryById(int id) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "select * from sub_task where id=?";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, id);
            rs = ps.executeQuery();

            SubTask subTask = new SubTask();
            while (rs.next()) {
                subTask.setId(rs.getInt("id"));
                subTask.setSourceCluster(rs.getString("source_cluster"));
                subTask.setSourceDB(rs.getString("source_db"));
                subTask.setSourceTB(rs.getString("source_tb"));
                subTask.setTargetCluster(rs.getString("target_cluster"));
                subTask.setTargetDB(rs.getString("target_db"));
                subTask.setTargetTB(rs.getString("target_tb"));
                subTask.setMetadataSyncStatus(rs.getString("metadata_sync_status"));
                subTask.setMetadataSyncRetry(rs.getInt("metadata_sync_retry"));
                subTask.setMaindataSyncStatus(rs.getString("maindata_sync_status"));
                subTask.setMaindataSyncSourcePath(rs.getString("maindata_sync_source_path"));
                subTask.setMaindataSyncTargetPath(rs.getString("maindata_sync_target_path"));
                subTask.setMaindataSyncArgs(rs.getString("maindata_sync_args"));
                subTask.setMaindataSyncJobId(rs.getString("maindata_sync_job_id"));
                subTask.setMaindataSyncJobStatus(rs.getString("maindata_sync_job_status"));
                subTask.setMaindataSyncRetry(rs.getInt("maindata_sync_retry"));
                subTask.setMaindataSyncStart(rs.getString("maindata_sync_start"));
                subTask.setMaindataSyncStop(rs.getString("maindata_sync_stop"));
                subTask.setMainTaskId(rs.getInt("main_task_id"));
                subTask.setSubTaskStatus(rs.getString("sub_task_status"));
                subTask.setCreateTime(rs.getString("create_time"));
                subTask.setUpdateTime(rs.getString("update_time"));
                return subTask;
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

    public SubTask queryByMDBTable(int mainTaskId, String sourceCluster, String sourceDB, String sourceTB, String targetCluster, String targetDB, String targetTB) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "select * from sub_task where main_task_id=? " +
                    " and source_cluster=? " +
                    " and source_db=? " +
                    " and source_tb=? " +
                    " and target_cluster=? " +
                    " and target_db=? " +
                    " and target_tb=? " +
                    " and sub_task_status != 'discard' " +
                    " order by id desc limit 1";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, mainTaskId);
            ps.setString(2, sourceCluster);
            ps.setString(3, sourceDB);
            ps.setString(4, sourceTB);
            ps.setString(5, targetCluster);
            ps.setString(6, targetDB);
            ps.setString(7, targetTB);
            rs = ps.executeQuery();

            SubTask subTask = new SubTask();
            while (rs.next()) {
                subTask.setId(rs.getInt("id"));
                subTask.setSourceCluster(rs.getString("source_cluster"));
                subTask.setSourceDB(rs.getString("source_db"));
                subTask.setSourceTB(rs.getString("source_tb"));
                subTask.setTargetCluster(rs.getString("target_cluster"));
                subTask.setTargetDB(rs.getString("target_db"));
                subTask.setTargetTB(rs.getString("target_tb"));
                subTask.setMetadataSyncStatus(rs.getString("metadata_sync_status"));
                subTask.setMetadataSyncRetry(rs.getInt("metadata_sync_retry"));
                subTask.setMaindataSyncStatus(rs.getString("maindata_sync_status"));
                subTask.setMaindataSyncSourcePath(rs.getString("maindata_sync_source_path"));
                subTask.setMaindataSyncTargetPath(rs.getString("maindata_sync_target_path"));
                subTask.setMaindataSyncArgs(rs.getString("maindata_sync_args"));
                subTask.setMaindataSyncJobId(rs.getString("maindata_sync_job_id"));
                subTask.setMaindataSyncJobStatus(rs.getString("maindata_sync_job_status"));
                subTask.setMaindataSyncRetry(rs.getInt("maindata_sync_retry"));
                subTask.setMaindataSyncStart(rs.getString("maindata_sync_start"));
                subTask.setMaindataSyncStop(rs.getString("maindata_sync_stop"));
                subTask.setMainTaskId(rs.getInt("main_task_id"));
                subTask.setCreateTime(rs.getString("create_time"));
                subTask.setUpdateTime(rs.getString("update_time"));
                return subTask;
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

    public int update(SubTask subTask) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "update sub_task set " +
                    "metadata_sync_status=?" +
                    ",metadata_sync_retry=?" +
                    ",maindata_sync_status=?" +
                    ",maindata_sync_job_id=?" +
                    ",maindata_sync_job_status=?" +
                    ",maindata_sync_retry=?" +
                    ",maindata_sync_start=?" +
                    ",maindata_sync_stop=?" +
                    ",maindata_sync_args=? "+
                    " where id=?";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setString(1, subTask.getMetadataSyncStatus());
            ps.setInt(2, subTask.getMetadataSyncRetry());
            ps.setString(3, subTask.getMaindataSyncStatus());
            ps.setString(4, subTask.getMaindataSyncJobId());
            ps.setString(5, subTask.getMaindataSyncJobStatus());
            ps.setInt(6, subTask.getMaindataSyncRetry());
            ps.setString(7, subTask.getMaindataSyncStart());
            ps.setString(8, subTask.getMaindataSyncStop());
            ps.setString(9,subTask.getMaindataSyncArgs());
            ps.setInt(10, subTask.getId());

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

    public int updateSubTaskMetadataSyncStatus(int subTaskId, String status) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "update sub_task set " +
                    "metadata_sync_status=?" +
                    " where id=?";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setString(1, status);
            ps.setInt(2, subTaskId);
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

    public int updateAbnormalSubTask(SubTask subTask) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "update sub_task " +
                    "set sub_task_status='discard' " +
                    " where source_cluster=?" +
                    " and source_db=?" +
                    " and source_tb=?" +
                    " and target_cluster=?" +
                    " and target_db=?" +
                    " and target_tb=?" +
                    " and metadata_sync_status='success'" +
                    " and maindata_sync_status='wait'";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setString(1, subTask.getSourceCluster());
            ps.setString(2, subTask.getSourceDB());
            ps.setString(3, subTask.getSourceTB());
            ps.setString(4, subTask.getTargetCluster());
            ps.setString(5, subTask.getTargetDB());
            ps.setString(6, subTask.getTargetTB());
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

    public int updateFailSubTask(SubTask subTask) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "update sub_task " +
                    "set sub_task_status='discard' " +
                    "where source_cluster=?" +
                    " and source_db=?" +
                    " and source_tb=?" +
                    " and target_cluster=?" +
                    " and target_db=?" +
                    " and target_tb=?" +
                    " and main_task_id=?" +
                    " and metadata_sync_status='success'" +
                    " and maindata_sync_status='fail'";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setString(1, subTask.getSourceCluster());
            ps.setString(2, subTask.getSourceDB());
            ps.setString(3, subTask.getSourceTB());
            ps.setString(4, subTask.getTargetCluster());
            ps.setString(5, subTask.getTargetDB());
            ps.setString(6, subTask.getTargetTB());
            ps.setInt(7, subTask.getMainTaskId());
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

    /**
     * 重置主数据同步次数为0
     *
     * @param id
     * @return
     */
    public int resetMaindataSyncRetry(int id) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "update sub_task " +
                    "set maindata_sync_retry=0 " +
                    "where id=?";
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

    public ArrayList<SubTask> queryByMainTaskId(int id) {
        ArrayList<SubTask> subTasks = new ArrayList<SubTask>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "select * from sub_task where main_task_id=? and sub_task_status!='discard'";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, id);
            rs = ps.executeQuery();

            while (rs.next()) {
                SubTask subTask = new SubTask();
                subTask.setId(rs.getInt("id"));
                subTask.setSourceCluster(rs.getString("source_cluster"));
                subTask.setSourceDB(rs.getString("source_db"));
                subTask.setSourceTB(rs.getString("source_tb"));
                subTask.setTargetCluster(rs.getString("target_cluster"));
                subTask.setTargetDB(rs.getString("target_db"));
                subTask.setTargetTB(rs.getString("target_tb"));
                subTask.setMetadataSyncStatus(rs.getString("metadata_sync_status"));
                subTask.setMetadataSyncRetry(rs.getInt("metadata_sync_retry"));
                subTask.setMaindataSyncStatus(rs.getString("maindata_sync_status"));
                subTask.setMaindataSyncSourcePath(rs.getString("maindata_sync_source_path"));
                subTask.setMaindataSyncTargetPath(rs.getString("maindata_sync_target_path"));
                subTask.setMaindataSyncArgs(rs.getString("maindata_sync_args"));
                subTask.setMaindataSyncJobId(rs.getString("maindata_sync_job_id"));
                subTask.setMaindataSyncJobStatus(rs.getString("maindata_sync_job_status"));
                subTask.setMaindataSyncRetry(rs.getInt("maindata_sync_retry"));
                subTask.setMaindataSyncStart(rs.getString("maindata_sync_start"));
                subTask.setMaindataSyncStop(rs.getString("maindata_sync_stop"));
                subTask.setMainTaskId(rs.getInt("main_task_id"));
                subTask.setCreateTime(rs.getString("create_time"));
                subTask.setUpdateTime(rs.getString("update_time"));

                subTasks.add(subTask);
            }
            return subTasks;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs, ps, conn);
        }
        return null;
    }

    public int queryCntByMainTaskId(int id) {
        ArrayList<SubTask> subTasks = new ArrayList<SubTask>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "select count(*) from sub_task where main_task_id=? and sub_task_status!='discard'";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, id);
            rs = ps.executeQuery();

            while (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs, ps, conn);
        }
        return -1;
    }

    public int querySuccessCntByMainTaskId(int id) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "select count(*) from sub_task where main_task_id=? " +
                    " and metadata_sync_status='success' " +
                    " and maindata_sync_status='success'";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, id);
            rs = ps.executeQuery();

            while (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs, ps, conn);
        }
        return -1;
    }

    public ArrayList<SubTask> querySuccessByMainTaskId(int id) {
        ArrayList<SubTask> subTasks = new ArrayList<SubTask>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "select * from sub_task where main_task_id=? " +
                    " and metadata_sync_status='success' " +
                    " and (maindata_sync_status='success' or maindata_sync_status = 'viewNoDo')";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, id);
            rs = ps.executeQuery();
            while (rs.next()) {
                SubTask subTask = new SubTask();
                subTask.setId(rs.getInt("id"));
                subTask.setSourceCluster(rs.getString("source_cluster"));
                subTask.setSourceDB(rs.getString("source_db"));
                subTask.setSourceTB(rs.getString("source_tb"));
                subTask.setTargetCluster(rs.getString("target_cluster"));
                subTask.setTargetDB(rs.getString("target_db"));
                subTask.setTargetTB(rs.getString("target_tb"));
                subTask.setMetadataSyncStatus(rs.getString("metadata_sync_status"));
                subTask.setMetadataSyncRetry(rs.getInt("metadata_sync_retry"));
                subTask.setMaindataSyncStatus(rs.getString("maindata_sync_status"));
                subTask.setMaindataSyncSourcePath(rs.getString("maindata_sync_source_path"));
                subTask.setMaindataSyncTargetPath(rs.getString("maindata_sync_target_path"));
                subTask.setMaindataSyncArgs(rs.getString("maindata_sync_args"));
                subTask.setMaindataSyncJobId(rs.getString("maindata_sync_job_id"));
                subTask.setMaindataSyncJobStatus(rs.getString("maindata_sync_job_status"));
                subTask.setMaindataSyncRetry(rs.getInt("maindata_sync_retry"));
                subTask.setMaindataSyncStart(rs.getString("maindata_sync_start"));
                subTask.setMaindataSyncStop(rs.getString("maindata_sync_stop"));
                subTask.setMainTaskId(rs.getInt("main_task_id"));
                subTask.setCreateTime(rs.getString("create_time"));
                subTask.setUpdateTime(rs.getString("update_time"));

                subTasks.add(subTask);
            }
            return subTasks;
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
     *
     * @param id 主任务ID
     * @return 同步失败的子任务
     */
    public ArrayList<SubTask> queryFailedCntByMainTaskId(int id) {
        ArrayList<SubTask> subTasks = new ArrayList<SubTask>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "select * from sub_task where main_task_id=? and " +
                    "(metadata_sync_status != 'success' or (maindata_sync_status != 'success' and maindata_sync_status != 'viewNoDo'))";
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, id);
            rs = ps.executeQuery();

            while (rs.next()) {
                SubTask subTask = new SubTask();
                subTask.setId(rs.getInt("id"));
                subTask.setSourceCluster(rs.getString("source_cluster"));
                subTask.setSourceDB(rs.getString("source_db"));
                subTask.setSourceTB(rs.getString("source_tb"));
                subTask.setTargetCluster(rs.getString("target_cluster"));
                subTask.setTargetDB(rs.getString("target_db"));
                subTask.setTargetTB(rs.getString("target_tb"));
                subTask.setMetadataSyncStatus(rs.getString("metadata_sync_status"));
                subTask.setMetadataSyncRetry(rs.getInt("metadata_sync_retry"));
                subTask.setMaindataSyncStatus(rs.getString("maindata_sync_status"));
                subTask.setMaindataSyncSourcePath(rs.getString("maindata_sync_source_path"));
                subTask.setMaindataSyncTargetPath(rs.getString("maindata_sync_target_path"));
                subTask.setMaindataSyncArgs(rs.getString("maindata_sync_args"));
                subTask.setMaindataSyncJobId(rs.getString("maindata_sync_job_id"));
                subTask.setMaindataSyncJobStatus(rs.getString("maindata_sync_job_status"));
                subTask.setMaindataSyncRetry(rs.getInt("maindata_sync_retry"));
                subTask.setMaindataSyncStart(rs.getString("maindata_sync_start"));
                subTask.setMaindataSyncStop(rs.getString("maindata_sync_stop"));
                subTask.setMainTaskId(rs.getInt("main_task_id"));
                subTask.setCreateTime(rs.getString("create_time"));
                subTask.setUpdateTime(rs.getString("update_time"));

                subTasks.add(subTask);
            }
            return subTasks;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs, ps, conn);
        }
        return null;
    }
}
