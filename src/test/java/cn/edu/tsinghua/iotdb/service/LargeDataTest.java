package cn.edu.tsinghua.iotdb.service;

import static cn.edu.tsinghua.iotdb.service.TestUtils.count;
import static cn.edu.tsinghua.iotdb.service.TestUtils.max_time;
import static cn.edu.tsinghua.iotdb.service.TestUtils.max_value;
import static cn.edu.tsinghua.iotdb.service.TestUtils.min_time;
import static cn.edu.tsinghua.iotdb.service.TestUtils.min_value;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.query.engine.AggregateEngine;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;



public class LargeDataTest {

    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d1s0 = "root.vehicle.d1.s0";

    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d0s4 = "root.vehicle.d0.s4";
    private final String d1s1 = "root.vehicle.d1.s1";
    
    private static String[] stringValue = new String[]{"A", "B", "C", "D", "E"};

    private static String[] create_sql = new String[]{
            "SET STORAGE GROUP TO root.vehicle",

            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",

            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
    };

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;
    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    private int maxNumberOfPointsInPage;
    private int pageSizeInByte;
    private int groupSizeInByte;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            AggregateEngine.aggregateFetchSize = 4000;

            // use small page setting
            // origin value
            maxNumberOfPointsInPage = tsFileConfig.maxNumberOfPointsInPage;
            pageSizeInByte = tsFileConfig.pageSizeInByte;
            groupSizeInByte = tsFileConfig.groupSizeInByte;
            // new value
            tsFileConfig.maxNumberOfPointsInPage = 100;
            tsFileConfig.pageSizeInByte = 1024 * 1024 * 15;
            tsFileConfig.groupSizeInByte = 1024 * 1024 * 100;

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
            //recovery value
            tsFileConfig.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
            tsFileConfig.pageSizeInByte = pageSizeInByte;
            tsFileConfig.groupSizeInByte = groupSizeInByte;
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException, FileNotFoundException {
        //PrintStream ps = new PrintStream(new FileOutputStream("src/test/resources/ha.txt"));
        //System.setOut(ps);

        if (testFlag) {
            Thread.sleep(5000);
            insertSQL();

            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            selectAllTest();
            aggregationTest();
            groupByTest();
            allNullSeriesAggregationTest();

            allNullSeriesGroupByTest();

            negativeValueTest();

            fixBigGroupByClassFormNumberTest();

            seriesTimeDigestTest();

            connection.close();
        }
    }

    private void selectAllTest() throws ClassNotFoundException, SQLException, FileNotFoundException {

        String selectSql = "select s0 from root.vehicle.d0 where s0 >= 20";

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(selectSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0);
                cnt++;
            }
            //System.out.println("select ====== " + cnt);
            assertEquals(16340, cnt);
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

    private void aggregationTest() throws ClassNotFoundException, SQLException, FileNotFoundException {

        String countSql = "select count(s0) from root.vehicle.d0 where s0 >= 20";

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(countSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0));
                assertEquals("16340", resultSet.getString(count(d0s0)));
                cnt++;
            }
            assertEquals(1, cnt);
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

    private void groupByTest() throws ClassNotFoundException, SQLException, FileNotFoundException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            // (1). group by only with time filter
            String[] retArray = new String[]{
                    "2000,null",
                    "2100,null",
                    "2200,null",
                    "2300,49",
                    "2400,100",
                    "2500,null",
                    "3000,100",
                    "3100,100",
                    "3200,100",
                    "3300,100",
                    "3400,null",
                    "3500,null",
                    "3600,null",
                    "3700,null",
                    "3800,null",
                    "3900,null",
                    "4000,null",
            };
            String countSql = "select count(s0) from root.vehicle.d0 where time < 3400 and time > 2350 " +
                    "group by (100ms, 2000, [2000,2500], [3000, 4000])";
            boolean hasResultSet = statement.execute(countSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0));
                assertEquals(retArray[cnt], ans);
                //System.out.println("============ " + ans);
                cnt++;
            }
            assertEquals(17, cnt);
            statement.close();

            // (2). group by only with time filter and value filter
            retArray = new String[]{
                    "2000,null",
                    "2100,null",
                    "2200,null",
                    "2300,49",
                    "2400,100",
                    "2500,null",
                    "3000,28",
                    "3100,26",
                    "3200,30",
                    "3300,24",
                    "3400,null",
                    "3500,null",
                    "3600,null",
                    "3700,null",
                    "3800,null",
                    "3900,null",
                    "4000,null",
            };
            statement = connection.createStatement();
            countSql = "select count(s0) from root.vehicle.d0 where time < 3400 and time > 2350 and s2 > 15 " +
                    "group by (100ms, 2000, [2000,2500], [3000, 4000])";
            hasResultSet = statement.execute(countSql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0));
                assertEquals(retArray[cnt], ans);
                //System.out.println("============ " + ans);
                cnt++;
            }
            assertEquals(17, cnt);
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

    private void allNullSeriesAggregationTest() throws ClassNotFoundException, SQLException, FileNotFoundException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String sql;
            boolean hasResultSet;
            ResultSet resultSet;
            int cnt;
            String[] retArray = new String[]{};

            // (1). aggregation test : there is no value in series d1.s0 and no filter
            sql = "select count(s0),max_value(s0),min_value(s0),max_time(s0),min_time(s0) from root.vehicle.d1";
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d1s0))
                        + "," + resultSet.getString(max_value(d1s0)) + "," + resultSet.getString(min_value(d1s0))
                        + "," + resultSet.getString(max_time(d1s0)) + "," + resultSet.getString(min_time(d1s0));
                assertEquals("0,0,null,null,null,null", ans);
                //System.out.println("============ " + ans);
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();

            // (2). aggregation test : there is no value in series d1.s0 and have filter
            sql = "select count(s0),max_value(s0),min_value(s0),max_time(s0),min_time(s0) from root.vehicle.d1 where s0 > 1000000";
            statement = connection.createStatement();
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d1s0))
                        + "," + resultSet.getString(max_value(d1s0)) + "," + resultSet.getString(min_value(d1s0))
                        + "," + resultSet.getString(max_time(d1s0)) + "," + resultSet.getString(min_time(d1s0));
                assertEquals("0,0,null,null,null,null", ans);
                //System.out.println("0,0,null,null,null,null" + ans);
                cnt++;
            }
            assertEquals(1, cnt);
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

    private void allNullSeriesGroupByTest() throws ClassNotFoundException, SQLException, FileNotFoundException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String sql;
            boolean hasResultSet;
            ResultSet resultSet;
            int cnt;
            String[] retArray = new String[]{};

            // (1). group by test : the result is all null value
            sql = "select count(s0),max_value(s1) from root.vehicle.d0 where s2 > 1000000 " +
                    "group by (1000ms, 2000, [3500, 25000])";
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0)) + "," + resultSet.getString(max_value(d0s1));
                Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null", ans);
                // System.out.println("============ " + ans);
                cnt++;
            }
            Assert.assertEquals(23, cnt);
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

    private void negativeValueTest() throws ClassNotFoundException, SQLException, FileNotFoundException {

        String countSql = "select count(s0) from root.vehicle.d0 where s0 < 0";
        //String countSql = "select s0 from root.vehicle.d0 where s0 < 0";

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(countSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0));
                Assert.assertEquals("0,950", ans);
                cnt++;
            }
            assertEquals(1, cnt);
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

    private void fixBigGroupByClassFormNumberTest() throws ClassNotFoundException, SQLException {

        // remove formNumber in GroupByEngineNoFilter and GroupByEngineWithFilter

        String[] retArray = new String[]{
                "3000,100,99,0,3099,3000",
                "3100,100,99,0,3199,3100",
                "3200,100,99,0,3299,3200",
                "3300,100,99,0,3399,3300",
                "3400,100,99,0,3499,3400",
                "3500,100,99,0,3599,3500",
                "3600,100,99,0,3699,3600",
                "3700,100,99,0,3799,3700",
                "3800,100,99,0,3899,3800",
                "3900,100,99,0,3999,3900",
                "4000,1,0,0,4000,4000"
        };
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        ;
        boolean hasResultSet;
        Statement statement;

        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            hasResultSet = statement.execute("select min_value(s0),max_value(s0),max_time(s0),min_time(s0),count(s0)"
                    + "from root.vehicle.d0 group by(100ms, 0, [3000,4000])");
            assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(max_value(d0s0)) + "," + resultSet.getString(min_value(d0s0))
                        + "," + resultSet.getString(max_time(d0s0)) + "," + resultSet.getString(min_time(d0s0));
                // System.out.println(ans);
                assertEquals(retArray[cnt], ans);
                cnt++;
            }
            assertEquals(11, cnt);
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

    // https://github.com/thulab/iotdb/issues/192
    private void seriesTimeDigestTest() throws ClassNotFoundException, SQLException {

        // [3000, 13599] , [13700,23999]

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        ;
        boolean hasResultSet;
        Statement statement;

        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0 "
                    + "from root.vehicle.d0 where time > 22987");
            assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0);
                //System.out.println(ans);
                //assertEquals(retArray[cnt], ans);
                cnt++;
            }
            //System.out.println(cnt);
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

    private void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            for (String sql : create_sql) {
                statement.execute(sql);
            }

            // insert large amount of data    time range : 3000 ~ 13600
            for (int time = 3000; time < 13600; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
                statement.execute(sql);
            }

            statement.execute("flush");

            // insert large amount of data    time range : 13700 ~ 24000
            for (int time = 13700; time < 24000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
                statement.execute(sql);
            }

            statement.execute("merge");

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

            // overflow delete
            statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time < 3200");

            // overflow insert, time < 3000
            for (int time = 2000; time < 2500; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
                statement.execute(sql);
            }

            // overflow update
            statement.execute("UPDATE root.vehicle SET d0.s1 = 11111111 WHERE time > 23000 and time < 100100");


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
