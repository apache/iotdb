package cn.edu.tsinghua.iotdb.service;


import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class DeleteCreateSameSeriesTest {

    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";

    /**
     * These sqls create s1 firstly, and delete s1 later and create s1 with different data types.
     */
    private String[] sqls_1 = new String[]{
            "SET STORAGE GROUP TO root.vehicle",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "insert into root.vehicle.d0(timestamp,s0) values(100,100)",
            "insert into root.vehicle.d0(timestamp,s0) values(5,5)",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "insert into root.vehicle.d0(timestamp,s1) values(20,20.22)",
            "insert into root.vehicle.d0(timestamp,s1) values(2000,111.11)",
            "insert into root.vehicle.d0(timestamp,s1) values(100,333.33)",
            "DELETE TIMESERIES root.vehicle.d0.s1",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "insert into root.vehicle.d0(timestamp,s1) values(25,'fff')",
            "insert into root.vehicle.d0(timestamp,s1) values(20,'xxx')",
            "insert into root.vehicle.d0(timestamp,s1) values(100,'aaa')"
    };

    /**
     * These sqls create s1 firstly, and delete s1 later and create s1 with same data types.
     */
    private String[] sqls_2 = new String[]{
            //"SET STORAGE GROUP TO root.vehicle",
//            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
//            "insert into root.vehicle.d0(timestamp,s0) values(100,100)",
//            "insert into root.vehicle.d0(timestamp,s0) values(5,5)",
//            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
//            "insert into root.vehicle.d0(timestamp,s1) values(20,20.22)",
//            "insert into root.vehicle.d0(timestamp,s1) values(2000,111.11)",
//            "insert into root.vehicle.d0(timestamp,s1) values(100,333.33)",
            "DELETE TIMESERIES root.vehicle.d0.s1",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
            "insert into root.vehicle.d0(timestamp,s1) values(25, 55)",
            "insert into root.vehicle.d0(timestamp,s1) values(20, 66.7)",
            "insert into root.vehicle.d0(timestamp,s1) values(100, 88.0)",
            "merge"
    };

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(5000);
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException {
        if (testFlag) {
            Thread.sleep(5000);
            insertSQL(sqls_1);
            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            selectTest_1();
            insertSQL(sqls_2);
            selectTest_2();
            connection.close();
        }
    }


    private void insertSQL(String[] sqls) throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.execute(sql);
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void selectTest_1() throws ClassNotFoundException, SQLException {
        String[] selectResult = new String[]{
                "5,5,null",
                "20,null,xxx",
                "25,null,fff",
                "100,100,aaa"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select * from root");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1);
                // System.out.println(ans);
                Assert.assertEquals(selectResult[cnt], ans);
                cnt++;
            }
            Assert.assertEquals(4, cnt);
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void selectTest_2() throws ClassNotFoundException, SQLException {
        String[] selectResult = new String[]{
                "5,5,null",
                "20,null,66.7",
                "25,null,55.0",
                "100,100,88.0"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select * from root");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1);
                // System.out.println(ans);
                Assert.assertEquals(selectResult[cnt], ans);
                cnt++;
            }
            Assert.assertEquals(4, cnt);
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
