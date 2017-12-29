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

/**
 * Multiple aggregation with filter test.
 */
public class AggregationSmallDataTest {
    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d0s4 = "root.vehicle.d0.s4";
    private final String d1s0 = "root.vehicle.d1.s0";

    private static String[] sqls = new String[]{

            "SET STORAGE GROUP TO root.vehicle",
            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",

            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",


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

            "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
            "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",

            "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
            "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
    };


    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            AggregateEngine.aggregateFetchSize = 2;
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
            insertSQL();

            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            countAggreWithSingleFilterTest();
            minTimeAggreWithSingleFilterTest();
            maxTimeAggreWithSingleFilterTest();
            minValueAggreWithSingleFilterTest();
            maxValueAggreWithSingleFilterTest();
            meanAggreWithSingleFilterTest();
            sumAggreWithSingleFilterTest();
            firstAggreWithSingleFilterTest();

            countOnlyTimeFilterTest();

            connection.close();
        }
    }

    private void firstAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,33333,180"
        };
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select first(s0),first(s1) from root.vehicle.d0 where s2 >= 3.33");
            //boolean hasResultSet = statement.execute("select count(s3) from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(first(d0s0))+ "," + resultSet.getString(first(d0s1));
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

    private void sumAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,55555.0,55934.0,1028.9899849891663"
        };
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select sum(s0),sum(s1),sum(s2) from root.vehicle.d0 where s2 >= 3.33");
            //boolean hasResultSet = statement.execute("select count(s3) from root.vehicle.d0 where s1 >= 0");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(sum(d0s0))
                        + "," + resultSet.getString(sum(d0s1)) + "," + resultSet.getString(sum(d0s2));
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

    private void meanAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,27777.5,18644.666666666668,205.79799699783325"
        };
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select mean(s0),mean(s1),mean(s2) from root.vehicle.d0 where s2 >= 3.33");
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
                "0,2,3,5,1,0"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),count(s1),count(s2),count(s3),count(s4) from root.vehicle.d0 where s2 >= 3.33");
            // System.out.println(hasResultSet + "...");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                        + "," + resultSet.getString(count(d0s3)) + "," + resultSet.getString(count(d0s4));
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

    private void minTimeAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,104,1,2,101,100"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select min_time(s0),min_time(s1),min_time(s2),min_time(s3),min_time(s4) from root.vehicle.d0 " +
                    "where s1 < 50000 and s1 != 100");

            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(min_time(d0s0))
                        + "," + resultSet.getString(min_time(d0s1)) + "," + resultSet.getString(min_time(d0s2))
                        + "," + resultSet.getString(min_time(d0s3)) + "," + resultSet.getString(min_time(d0s4));
                // System.out.println("============ " + ans);
                Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
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

    private void maxTimeAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,105,105,105,102,100"
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

    private void minValueAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,33333,180,2.22,tomorrow is another day,true"
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

    private void maxValueAggreWithSingleFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,33333,40000,11.11,tomorrow is another day,true"
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
                //System.out.println("============ " + ans);
                //Assert.assertEquals(ans, retArray[cnt]);
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

    private void countAggreWithMultiMultiFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1,null,1101,null,null,999",
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0) from root.vehicle.d0 where s2 >= 3.33");
            // System.out.println(hasResultSet + "...");
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0));
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
            boolean hasResultSet = statement.execute("select * from root");
            // System.out.println(hasResultSet + "...");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                            + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d1s0);
                    // System.out.println(ans);
                    Assert.assertEquals(ans, retArray[cnt]);
                    cnt++;
                }
                Assert.assertEquals(17, cnt);
            }
            statement.close();

            retArray = new String[]{
                    "100,true"
            };
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s4 from root.vehicle.d0");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s4);
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

    private void countOnlyTimeFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "0,3,7,4,5,1"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),count(s1),count(s2),count(s3),count(s4) " +
                    "from root.vehicle.d0 where time >= 3 and time <= 106");

            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                        + "," + resultSet.getString(count(d0s3)) + "," + resultSet.getString(count(d0s4));
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

    public static void insertSQL() throws ClassNotFoundException, SQLException {
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
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

}
