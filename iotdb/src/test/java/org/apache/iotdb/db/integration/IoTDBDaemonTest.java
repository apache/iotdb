package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.integration.Constant.TIMESTAMP_STR;
import static org.apache.iotdb.db.integration.Constant.d0s0;
import static org.apache.iotdb.db.integration.Constant.d0s1;
import static org.apache.iotdb.db.integration.Constant.d0s2;
import static org.apache.iotdb.db.integration.Constant.d0s3;
import static org.apache.iotdb.db.integration.Constant.d0s4;
import static org.apache.iotdb.db.integration.Constant.d1s0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test.
 * All test which will start the IoTDB server should be defined as integration test.
 */
public class IoTDBDaemonTest {

    private static IoTDB deamon;

    private static Connection connection;

    private static String[] sqls = new String[]{

            "SET STORAGE GROUP TO root.vehicle.d0",
            "SET STORAGE GROUP TO root.vehicle.d1",

            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",

            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",

            "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
            "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
            "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
            "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
            "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
            "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
            "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",

            "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
            "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
            "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
            "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
            "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
            "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
            "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
            "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
            "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
            "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
            "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",

            "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
            "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
            "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
            "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
            "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
            "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
            "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",

            "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
            "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
            "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
            "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
            "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",

            "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
            "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",

            "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
            "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",

            "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
            "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
    };

    @BeforeClass
    public static void setUp() throws Exception {
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.closeMemControl();
        deamon = IoTDB.getInstance();
        deamon.active();
        EnvironmentUtils.envSetUp();

        insertData();
        connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        connection.close();
        deamon.stop();
        Thread.sleep(5000);

        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void selectAllSQLTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1,101,1101,null,null,999",
                "2,10000,40000,2.22,null,null",
                "3,null,null,3.33,null,null",
                "4,null,null,4.44,null,null",
                "50,10000,50000,null,null,null",
                "60,null,null,null,aaaaa,null",
                "70,null,null,null,bbbbb,null",
                "80,null,null,null,ccccc,null",
                "100,99,199,null,null,null",
                "101,99,199,null,ddddd,null",
                "102,80,180,10.0,fffff,null",
                "103,99,199,null,null,null",
                "104,90,190,null,null,null",
                "105,99,199,11.11,null,null",
                "106,99,null,null,null,null",
                "1000,22222,55555,1000.11,null,888",
                "946684800000,null,100,null,good,null"
        };

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select * from root");
            Assert.assertTrue(hasResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d1s0);
                Assert.assertEquals(retArray[cnt], ans);
                cnt++;
            }
            Assert.assertEquals(17, cnt);
            statement.close();

            retArray = new String[]{
                    "100,true"
            };
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s4 from root.vehicle.d0");
            Assert.assertTrue(hasResultSet);

            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s4);
                Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
            }
            Assert.assertEquals(1, cnt);
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

    @Test
    public void selectWildCardSQLTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "2,2.22",
                "3,3.33",
                "4,4.44",
                "102,10.0",
                "105,11.11",
                "1000,1000.11"};

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s2 from root.vehicle.*");
            Assert.assertTrue(hasResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s2);
                Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
            }
            Assert.assertEquals(6, cnt);

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

    @Test
    public void dnfErrorSQLTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1,101,1101",
                "2,10000,40000",
                "50,10000,50000",
                "100,99,199",
                "101,99,199",
                "102,80,180",
                "103,99,199",
                "104,90,190",
                "105,99,199"
        };

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0,s1 from root.vehicle.d0 where time < 106 and (s0 >= 60 or s1 <= 200)");
            Assert.assertTrue(hasResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1);
                assertEquals(retArray[cnt], ans);
                cnt++;
            }
            Assert.assertEquals(9, cnt);

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

    @Test
    public void selectAndOperatorTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1000,22222,55555,888"
        };

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            //TODO  select s0,s1 from root.vehicle.d0 where time > 106 and root.vehicle.d1.s0 > 100;
            boolean hasResultSet = statement.execute(
                    "select s0,s1 from root.vehicle.d0,root.vehicle.d1 where time > 106 and root.vehicle.d0.s0 > 100");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1) + ","
                        + resultSet.getString(d1s0);
                Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
            }
            Assert.assertEquals(1, cnt);
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

    @Test
    public void selectAndOpeCrossTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1000,22222,55555"};

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0,s1 from root.vehicle.d0 where time > 106 and root.vehicle.d1.s0 > 100");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1);
                Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
            }
            Assert.assertEquals(1, cnt);
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

    @Test
    public void selectOneColumnWithFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "102,180",
                "104,190",
                "946684800000,100"
        };

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            boolean hasTextMaxResultSet = statement.execute("select s1 from root.vehicle.d0 where s1 < 199");
            Assert.assertTrue(hasTextMaxResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s1);
                Assert.assertEquals(retArray[cnt++], ans);
            }
            Assert.assertEquals(3, cnt);

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

    private static void insertData() throws ClassNotFoundException, SQLException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
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
}
