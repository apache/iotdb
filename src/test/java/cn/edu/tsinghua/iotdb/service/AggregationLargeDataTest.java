package cn.edu.tsinghua.iotdb.service;


import static cn.edu.tsinghua.iotdb.service.TestUtils.*;
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
import cn.edu.tsinghua.iotdb.query.engine.AggregateEngine;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class AggregationLargeDataTest {
    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d0s4 = "root.vehicle.d0.s4";

    private static String[] createSql = new String[]{
            "SET STORAGE GROUP TO root.vehicle",
            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
    };

    private static String[] insertSql = new String[]{
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
            "DELETE FROM root.vehicle.d0.s0 WHERE time < 104",
            "UPDATE root.vehicle.d0 SET s0 = 33333 WHERE time < 106 and time > 103",

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
            "UPDATE root.vehicle.d0 SET s3 = 'tomorrow is another day' WHERE time >100 and time < 103",

            "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
            "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",

            // "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",

            "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
            "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
    };

    private static String[] sql_2 = new String[]{
            "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')"
    };

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.stop();
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
            insertSQL();

            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            selectAllSQLTest();
            lastAggreWithSingleFilterTest();
            meanAggreWithSingleFilterTest();
            sumAggreWithSingleFilterTest();
            firstAggreWithSingleFilterTest();
            countAggreWithSingleFilterTest();
            minMaxTimeAggreWithSingleFilterTest();
            minValueAggreWithSingleFilterTest();
            maxValueAggreWithSingleFilterTest();
            
            lastAggreWithMultiFilterTest();
            countAggreWithMultiFilterTest();
            minTimeAggreWithMultiFilterTest();
            maxTimeAggreWithMultiFilterTest();
            minValueAggreWithMultiFilterTest();
            maxValueAggreWithMultiFilterTest();
            meanAggreWithMultiFilterTest();
            sumAggreWithMultiFilterTest();
            firstAggreWithMultiFilterTest();
            connection.close();
        }
    }

    private void lastAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,9,39,63.0,E,true"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select last(s0),last(s1),last(s2),last(s3),last(s4)" +
                    " from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TestUtils.last(d0s0)) + ","
                        + resultSet.getString(TestUtils.last(d0s1)) + "," + resultSet.getString(TestUtils.last(d0s2))
                        + "," + resultSet.getString(TestUtils.last(d0s3)) + "," + resultSet.getString(TestUtils.last(d0s4));
                // System.out.println("============ " + ans);
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

    private void lastAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,9,39,63.0"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select last(s0),last(s1),last(s2)" +
                    " from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TestUtils.last(d0s0)) + ","
                        + resultSet.getString(TestUtils.last(d0s1)) + "," + resultSet.getString(TestUtils.last(d0s2));
                Assert.assertEquals(retArray[cnt], ans);
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


    private void sumAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,121538.0,156752.0,20254.43998503685"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select sum(s0),sum(s1),sum(s2)" +
                    " from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TestUtils.sum(d0s0)) + ","
                        + resultSet.getString(TestUtils.sum(d0s1)) + "," + resultSet.getString(TestUtils.sum(d0s2));
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
    
    private void firstAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,33333,1101,2.22,tomorrow is another day,true"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select first(s0),first(s1),first(s2),first(s3),first(s4)" +
                    " from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TestUtils.first(d0s0)) + ","
                        + resultSet.getString(TestUtils.first(d0s1)) + "," + resultSet.getString(TestUtils.first(d0s2))
                        + "," + resultSet.getString(TestUtils.first(d0s3)) + "," + resultSet.getString(TestUtils.first(d0s4));
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

    private void meanAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,165.80900409276944,211.82702702702701,27.59460488424639"
        };
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0,s1,s2 from root.vehicle.d0 where s1 >= 0");
            //boolean hasResultSet = statement.execute("select count(s3) from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0)
                        + "," + resultSet.getString(d0s1) + "," + resultSet.getString(d0s2);
                //System.out.println(ans);
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select mean(s0),mean(s1),mean(s2) from root.vehicle.d0 where s1 >= 0");
            //boolean hasResultSet = statement.execute("select count(s3) from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(mean(d0s0))
                        + "," + resultSet.getString(mean(d0s1)) + "," + resultSet.getString(mean(d0s2));
                //System.out.println("!!!!!============ " + ans);
                Assert.assertEquals(retArray[cnt], ans);
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

    private void countAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,733,740,734"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),count(s1),count(s2) from root.vehicle.d0 where s1 >= 0");
            //boolean hasResultSet = statement.execute("select count(s3) from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2));
                //System.out.println("!!!!!============ " + ans);
                Assert.assertEquals(retArray[cnt], ans);
                cnt++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();
        }catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void minMaxTimeAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,104,1,2,101,100"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select min_time(s0),min_time(s1),min_time(s2),min_time(s3),min_time(s4)" +
                    " from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TestUtils.min_time(d0s0)) + ","
                        + resultSet.getString(TestUtils.min_time(d0s1)) + "," + resultSet.getString(TestUtils.min_time(d0s2))
                        + "," + resultSet.getString(TestUtils.min_time(d0s3)) + "," + resultSet.getString(TestUtils.min_time(d0s4));
                Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            retArray = new String[]{
                    "0,3999,3999,3999,3599,100"
            };
            statement = connection.createStatement();
            hasResultSet = statement.execute("select max_time(s0),max_time(s1),max_time(s2),max_time(s3),max_time(s4) from root.vehicle.d0 " +
                    "where s1 < 50000 and s1 != 100");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_time(d0s0))
                        + "," + resultSet.getString(max_time(d0s1)) + "," + resultSet.getString(max_time(d0s2))
                        + "," + resultSet.getString(max_time(d0s3)) + "," + resultSet.getString(max_time(d0s4));
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

    private void minValueAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,0,0,0.0,B,true"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select min_value(s0),min_value(s1),min_value(s2),min_value(s3),min_value(s4) from root.vehicle.d0 " +
                    "where s1 < 50000 and s1 != 100");

            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(min_value(d0s0))
                            + "," + resultSet.getString(min_value(d0s1)) + "," + resultSet.getString(min_value(d0s2))
                            + "," + resultSet.getString(min_value(d0s3)) + "," + resultSet.getString(min_value(d0s4));
                    //System.out.println("============ " + ans);
                    Assert.assertEquals(ans, retArray[cnt]);
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
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

    private void maxValueAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,33333,40000,122.0,tomorrow is another day,true"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select max_value(s0),max_value(s1),max_value(s2),max_value(s3),max_value(s4) from root.vehicle.d0 " +
                    "where s1 < 50000 and s1 != 100");

            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d0s0))
                            + "," + resultSet.getString(max_value(d0s1)) + "," + resultSet.getString(max_value(d0s2))
                            + "," + resultSet.getString(max_value(d0s3)) + "," + resultSet.getString(max_value(d0s4));
                    //System.out.println("============ " + ans);
                    Assert.assertEquals(ans, retArray[cnt]);
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
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

    private void meanAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,121538.0,733,165.80900409276944,211.82702702702701,27.530176610078502"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select sum(s0),count(s0),mean(s0),mean(s1),mean(s2) from root.vehicle.d0 " +
                    "where s1 >= 0 or s2 < 10");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(sum(d0s0)) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(mean(d0s0))
                        + "," + resultSet.getString(mean(d0s1)) + "," + resultSet.getString(mean(d0s2));
                //System.out.println("!!!!!============ " + ans);
                Assert.assertEquals(retArray[cnt], ans);
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

    private void sumAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,121538.0,156752.0,20262.209985017776"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select sum(s0),sum(s1),sum(s2) from root.vehicle.d0 " +
                    "where s1 >= 0 or s2 < 10");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(sum(d0s0))
                        + "," + resultSet.getString(sum(d0s1)) + "," + resultSet.getString(sum(d0s2));
                //String ans = resultSet.getString(sum(d0s3));
                //System.out.println("!!!!!============ " + ans);
                Assert.assertEquals(retArray[cnt], ans);
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

    private void firstAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,33333,1101,2.22,tomorrow is another day,true"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select first(s0),first(s1),first(s2),first(s3),first(s4) from root.vehicle.d0 " +
                    "where s1 >= 0 or s2 < 10");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(first(d0s0))
                        + "," + resultSet.getString(first(d0s1)) + "," + resultSet.getString(first(d0s2))
                        + "," + resultSet.getString(first(d0s3)) + "," + resultSet.getString(first(d0s4));
                //String ans = resultSet.getString(first(d0s3));
                //System.out.println("!!!!!============ " + ans);
                Assert.assertEquals(retArray[cnt], ans);
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

    private void countAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,733,740,736,482,1"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),count(s1),count(s2),count(s3),count(s4) from root.vehicle.d0 " +
                    "where s1 >= 0 or s2 < 10");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                        + "," + resultSet.getString(count(d0s3)) + "," + resultSet.getString(count(d0s4));
                //String ans = resultSet.getString(count(d0s3));
                //System.out.println("!!!!!============ " + ans);
                Assert.assertEquals(retArray[cnt], ans);
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

    private void minTimeAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,104,1,2,101,100"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select min_time(s0),min_time(s1),min_time(s2),min_time(s3),min_time(s4)" +
                    " from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TestUtils.min_time(d0s0)) + ","
                        + resultSet.getString(TestUtils.min_time(d0s1)) + "," + resultSet.getString(TestUtils.min_time(d0s2))
                        + "," + resultSet.getString(TestUtils.min_time(d0s3)) + "," + resultSet.getString(TestUtils.min_time(d0s4));
                // System.out.println("============ " + ans);
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

    private void maxTimeAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,3999,3999,3999,3599,100"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select max_time(s0),max_time(s1),max_time(s2),max_time(s3),max_time(s4) from root.vehicle.d0 " +
                    "where s1 < 50000 and s1 != 100");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_time(d0s0))
                        + "," + resultSet.getString(max_time(d0s1)) + "," + resultSet.getString(max_time(d0s2))
                        + "," + resultSet.getString(max_time(d0s3)) + "," + resultSet.getString(max_time(d0s4));
                //System.out.println("============ " + ans);
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

    private void minValueAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,0,0,0.0,B,true"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select min_value(s0),min_value(s1),min_value(s2),min_value(s3),min_value(s4) from root.vehicle.d0 " +
                    "where s1 < 50000 and s1 != 100");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(min_value(d0s0))
                        + "," + resultSet.getString(min_value(d0s1)) + "," + resultSet.getString(min_value(d0s2))
                        + "," + resultSet.getString(min_value(d0s3)) + "," + resultSet.getString(min_value(d0s4));
                //System.out.println("============ " + ans);
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

    private void maxValueAggreWithMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,33333,40000,122.0,tomorrow is another day,true"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select max_value(s0),max_value(s1),max_value(s2),max_value(s3),max_value(s4) from root.vehicle.d0 " +
                    "where s1 < 50000 and s1 != 100");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d0s0))
                        + "," + resultSet.getString(max_value(d0s1)) + "," + resultSet.getString(max_value(d0s2))
                        + "," + resultSet.getString(max_value(d0s3)) + "," + resultSet.getString(max_value(d0s4));
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

    private static String[] stringValue = new String[]{"A", "B", "C", "D", "E"};

    public static void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : createSql) {
                statement.execute(sql);
            }

            // insert large amount of data
            for (int time = 3000; time < 3600; time++) {
                if (time % 5 == 0) {
                    continue;
                }

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

            // insert large amount of data
            for (int time = 3700; time < 4000; time++) {
                if (time % 6 == 0) {
                    continue;
                }

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
                statement.execute(sql);
            }

            statement.execute("merge");

            for (String sql : insertSql) {
                statement.execute(sql);
            }

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

    private void selectAllSQLTest() throws ClassNotFoundException, SQLException {
        //d0s0,d0s1,d0s2,d0s3,d1s0
        String[] retArray = new String[]{
                "1,null,1101,null,null,999",
                "2,null,40000,2.22,null,null",
                "3,null,null,3.33,null,null",
                "4,null,null,4.44,null,null",
                "50,null,50000,null,null,null",
                "60,null,null,null,aaaaa,null",
                "70,null,null,null,bbbbb,null",
                "80,null,null,null,ccccc,null",
                "100,null,199,null,null,null",
                "101,null,199,null,tomorrow is another day,null",
                "102,null,180,10.0,tomorrow is another day,null",
                "103,null,199,null,null,null",
                "104,33333,190,null,null,null",
                "105,33333,199,11.11,null,null",
                "106,99,null,null,null,null",
                "1000,22222,55555,1000.11,null,888",
                "946684800000,null,100,null,good,null"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0 from root.vehicle.d0 where s1 >= 0 or s2 < 10");
            Assert.assertTrue(hasResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                //String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                //       + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d1s0);
                //String ans =resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0);
                //System.out.println(ans + ",," + cnt);
                //Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
            }
            //System.out.println("----" + cnt);
            // Assert.assertEquals(17, cnt);
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
