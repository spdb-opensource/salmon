package com.spdb.hive.sync.util.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.spdb.hive.sync.util.property.PropUtil;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * project：hive-sync
 * package：com.spdb.hive.sync.util.db
 * author：zhouxh
 * time：2021-04-16 9:01
 * description：
 */
public class DBUtil {

    private static ComboPooledDataSource dataSource = new ComboPooledDataSource();
    static {
        try {
            String driver = PropUtil.getProValue("mysql.driver");
            String url = PropUtil.getProValue("mysql.url");
            if (!url.endsWith("/")) {
                url = url + "/";
            }
            String db = PropUtil.getProValue("mysql.db");
            String user = PropUtil.getProValue("mysql.user");
            String passwd = PropUtil.getProValue("mysql.passwd");

            dataSource.setDriverClass(driver);
            dataSource.setJdbcUrl(url + db + "?autoReconnect=true&useSSL=false");
            dataSource.setUser(user);
            dataSource.setPassword(passwd);
            dataSource.setInitialPoolSize(10);
            dataSource.setMinPoolSize(10);
            dataSource.setMaxPoolSize(10);
            dataSource.setMaxIdleTime(0);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @return 获得数据库连接
     */
    public static Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param conn
     */
    public static void close(Connection conn) {
        close(null, null, conn);
    }

    /**
     * @param ps
     * @param conn
     */
    public static void close(PreparedStatement ps, Connection conn) {
        close(null, ps, conn);
    }

    /**
     * @param rs
     * @param ps
     * @param conn
     */
    public static void close(ResultSet rs, PreparedStatement ps, Connection conn) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        System.out.println(getConnection());
    }
}
