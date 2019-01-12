package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test.
 * All test which will start the IoTDB server should be defined as integration test.
 */
public class IoTDBMultiSeriesTest {

    private static IoTDB deamon;

    private static boolean testFlag = Constant.testFlag;
    private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    private static int maxNumberOfPointsInPage;
    private static int pageSizeInByte;
    private static int groupSizeInByte;
    private static Connection connection;

    @BeforeClass
    public static void setUp() throws Exception {

        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.closeMemControl();

        // use small page setting
        // origin value
        maxNumberOfPointsInPage = tsFileConfig.maxNumberOfPointsInPage;
        pageSizeInByte = tsFileConfig.pageSizeInByte;
        groupSizeInByte = tsFileConfig.groupSizeInByte;

        // new value
        tsFileConfig.maxNumberOfPointsInPage = 1000;
        tsFileConfig.pageSizeInByte = 1024 * 150;
        tsFileConfig.groupSizeInByte = 1024 * 1000;

        deamon = IoTDB.getInstance();
        deamon.active();
        EnvironmentUtils.envSetUp();

        Thread.sleep(5000);
        insertData();

        connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");

    }

    @AfterClass
    public static void tearDown() throws Exception {

        connection.close();

        deamon.stop();
        Thread.sleep(5000);

        //recovery value
        tsFileConfig.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
        tsFileConfig.pageSizeInByte = pageSizeInByte;
        tsFileConfig.groupSizeInByte = groupSizeInByte;

        EnvironmentUtils.cleanEnv();
    }

    // "select * from root.vehicle" : test select wild data
    @Test
    public void selectAllTest() throws ClassNotFoundException, SQLException {
        String selectSql = "select * from root.vehicle";

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(selectSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(Constant.TIMESTAMP_STR) + "," + resultSet.getString(Constant.d0s0)
                        + "," + resultSet.getString(Constant.d0s1) + "," + resultSet.getString(Constant.d0s2) + "," + resultSet.getString(Constant.d0s3)
                        + "," + resultSet.getString(Constant.d0s4) + "," + resultSet.getString(Constant.d0s5);
                cnt++;
            }

            assertEquals(23400, cnt);
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

    // "select s0 from root.vehicle.d0 where s0 >= 20" : test select same series with same series filter
    @Test
    public void selectOneSeriesWithValueFilterTest() throws ClassNotFoundException, SQLException {

        String selectSql = "select s0 from root.vehicle.d0 where s0 >= 20";

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(selectSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(Constant.TIMESTAMP_STR) + "," + resultSet.getString(Constant.d0s0);
                //System.out.println("===" + ans);
                cnt++;
            }
            assertEquals(16440, cnt);
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

    // "select s0 from root.vehicle.d0 where time > 22987 " : test select clause with only global time filter
    @Test
    public void seriesGlobalTimeFilterTest() throws ClassNotFoundException, SQLException {

        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
        boolean hasResultSet;
        Statement statement;

        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0 from root.vehicle.d0 where time > 22987");
            assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(Constant.TIMESTAMP_STR) + "," + resultSet.getString(Constant.d0s0);
                //System.out.println(ans);
                cnt++;
            }

            assertEquals(3012, cnt);
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

    // "select s1 from root.vehicle.d0 where s0 < 111" : test select clause with different series filter
    @Test
    public void crossSeriesReadUpdateTest() throws ClassNotFoundException, SQLException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
        boolean hasResultSet;
        Statement statement;

        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s1 from root.vehicle.d0 where s0 < 111");
            assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                long time = Long.valueOf(resultSet.getString(Constant.TIMESTAMP_STR));
                String value = resultSet.getString(Constant.d0s1);
                if (time > 200900) {
                    assertEquals("7777", value);
                }
                //String ans = resultSet.getString(d0s1);
                cnt++;
            }
            assertEquals(22800, cnt);
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

    private static void insertData() throws ClassNotFoundException, SQLException, InterruptedException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            for (String sql : Constant.create_sql) {
                statement.execute(sql);
            }

            // insert large amount of data time range : 13700 ~ 24000
            for (int time = 13700; time < 24000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
                statement.execute(sql);
            }

            // insert large amount of data    time range : 3000 ~ 13600
            for (int time = 3000; time < 13600; time++) {
                //System.out.println("===" + time);
                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, Constant.stringValue[time % 5]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time, Constant.booleanValue[time % 2]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
                statement.execute(sql);
            }

            statement.execute("flush");
            //statement.execute("merge");

            Thread.sleep(5000);

            // buffwrite data, unsealed file
            for (int time = 100000; time < 101000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
                statement.execute(sql);
            }

            statement.execute("flush");

            // bufferwrite data, memory data
            for (int time = 200000; time < 201000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
                statement.execute(sql);
            }

            // unseq insert, time < 3000
            for (int time = 2000; time < 2500; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, Constant.stringValue[time % 5]);
                statement.execute(sql);
            }

            // seq insert, time > 200000
            for (int time = 200900; time < 201000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 7777);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 8888);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, "goodman");
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time, Constant.booleanValue[time % 2]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, 9999);
                statement.execute(sql);
            }

            // overflow delete
            // statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time < 3200");

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
