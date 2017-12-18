package cn.edu.tsinghua.iotdb.service;

import static cn.edu.tsinghua.iotdb.service.TestUtils.max_value;
import static cn.edu.tsinghua.iotdb.service.TestUtils.min_value;
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

/**
 * Just used for integration test.
 */
public class DaemonTest {
    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d0s4 = "root.vehicle.d0.s4";
    private final String d1s0 = "root.vehicle.d1.s0";
    private final String d1s1 = "root.vehicle.d1.s1";

    private String count(String path) {
        return String.format("count(%s)", path);
    }

    private String[] sqls = new String[]{
            "SET STORAGE GROUP TO root.vehicle",
            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
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
            "UPDATE root.vehicle SET d0.s0 = 33333 WHERE time < 106 and time > 103",

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
            "UPDATE root.vehicle SET d0.s3 = 'tomorrow is another day' WHERE time >100 and time < 103",

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
            deamon = new IoTDB();
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
    public void test() {
        if (testFlag) {
            try {
                Thread.sleep(5000);
                insertSQL();

                Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

                selectAllSQLTest();
                dnfErrorSQLTest();
                selectWildCardSQLTest();
                selectAndOperatorTest();
                selectAndOpeCrossTest();
                aggregationTest();
                selectOneColumnWithFilterTest();
                multiAggregationTest();
                crossReadTest();

                connection.close();
            } catch (ClassNotFoundException | SQLException | InterruptedException e) {
                fail(e.getMessage());
            }
        }
    }

    private void insertSQL() throws ClassNotFoundException, SQLException {
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

    private void multiAggregationTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "11,6,6"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s1),count(s2),count(s3) from root.vehicle.d0");
            Assert.assertTrue(hasResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString("count(" + d0s1 + ")") + ","
                        + resultSet.getString("count(" + d0s2 + ")") + ","
                        + resultSet.getString("count(" + d0s3 + ")");
                Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            // the statement has same columns and same aggregation
            retArray = new String[]{
                    "11,11,6,1000.11"
            };
            statement = connection.createStatement();
            hasResultSet = statement.execute("select count(s1),count(s1),count(s3),max_value(s2) from root.vehicle.d0");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString("count(" + d0s1 + ")") + ","
                        + resultSet.getString("count(" + d0s1 + ")") + ","
                        + resultSet.getString("count(" + d0s3 + ")") + ","
                        + resultSet.getString("max_value(" + d0s2 + ")");
                //System.out.println("!!" + ans);
                Assert.assertEquals(retArray[cnt], ans);
                cnt++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            // the statement has same columns and different aggregation
            retArray = new String[]{
                    "11,55555,946684800000,6"
            };
            statement = connection.createStatement();
            hasResultSet = statement.execute("select count(s1),max_value(s1),max_time(s1),count(s3) from root.vehicle.d0");
            Assert.assertTrue(hasResultSet);

            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(count(d0s1)) + ","
                        + resultSet.getString("max_value(" + d0s1 + ")") + ","
                        + resultSet.getString("max_time(" + d0s1 + ")") + ","
                        + resultSet.getString("count(" + d0s3 + ")");
                //System.out.println("==" + ans);
                Assert.assertEquals(ans, retArray[cnt]);
                cnt++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();
        } catch (Exception e) {
            fail(e.getMessage());
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void selectAllSQLTest() throws ClassNotFoundException, SQLException {
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

    private void dnfErrorSQLTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "100,null,199",
                "101,null,199",
                "102,null,180",
                "103,null,199",
                "104,33333,190",
                "105,33333,199"};

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0,s1 from root.vehicle.d0 where time < 106 and (s0 >= 60 or s1 <= 200)");
            Assert.assertTrue(hasResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1);
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

    private void selectWildCardSQLTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "2,2.22",
                "3,3.33",
                "4,4.44",
                "102,10.0",
                "105,11.11",
                "1000,1000.11"};

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
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

    private void selectAndOperatorTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1000,22222,55555,888"};

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            //TODO  select s0,s1 from root.vehicle.d0 where time > 106 and root.vehicle.d1.s0 > 100;
            boolean hasResultSet = statement.execute("select s0,s1 from root.vehicle.d0,root.vehicle.d1 where time > 106 and root.vehicle.d0.s0 > 100");
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

    private void selectAndOpeCrossTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1000,22222,55555"};

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
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

    private void aggregationTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "tomorrow is another day",
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasTextMaxResultSet = statement.execute("select max_value(s3) from root.vehicle.d0");
            Assert.assertTrue(hasTextMaxResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(max_value(d0s3));
                Assert.assertEquals(retArray[0], ans);
                cnt++;
            }
            Assert.assertEquals(1, cnt);

            statement.close();

            statement = connection.createStatement();
            boolean hasTextMinResultSet = statement.execute("select min_value(s3) from root.vehicle.d0");
            Assert.assertTrue(hasTextMinResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(min_value(d0s3));
                // System.out.println("=====" + ans);
                Assert.assertEquals("aaaaa", ans);
                cnt++;
            }
            Assert.assertEquals(cnt, 1);
            statement.close();

            statement = connection.createStatement();
            boolean hasMultiAggreResult = statement.execute("select min_value(s0) from root.vehicle.d0,root.vehicle.d1");
            Assert.assertTrue(hasMultiAggreResult);

            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans1 = resultSet.getString(min_value(d0s0));
                String ans2 = resultSet.getString(min_value(d1s0));
                Assert.assertEquals("99", ans1);
                Assert.assertEquals("888", ans2);
                cnt++;
            }
            Assert.assertEquals(cnt, 1);

            statement.close();
        } catch (Exception e) {
        	System.out.println(e.getMessage());
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void selectOneColumnWithFilterTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "102,180",
                "104,190",
                "946684800000,100"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            boolean hasTextMaxResultSet = statement.execute("select s1 from root.vehicle.d0 where s1 < 199");
            Assert.assertTrue(hasTextMaxResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s1);
                Assert.assertEquals(ans, retArray[cnt++]);
            }
            Assert.assertEquals(cnt, 3);

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

    private void crossReadTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1,101,1101,7.0",
                "2,198,198,8.0",
                "3,null,null,3.33",
                "4,null,null,4.44",
                "100,300,199,19.0",
                "101,99,199,10.0",
                "102,80,180,18.0",
                "103,99,199,12.0",
                "104,90,190,13.0",
                "105,99,199,14.0"
        };

        String[] sqls = new String[]{
                "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
                "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
                "insert into root.vehicle.d0(timestamp,s0) values(100,300)",
                "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
                "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
                "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
                "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
                "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
                "insert into root.vehicle.d0(timestamp,s0) values(106,99)",

                "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
                "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
                "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
                "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
                "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
                "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
                "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
                "insert into root.vehicle.d0(timestamp,s1) values(105,199)",

                "insert into root.vehicle.d0(timestamp,s1) values(51,51)",
                "insert into root.vehicle.d0(timestamp,s1) values(52,52)",
                "insert into root.vehicle.d0(timestamp,s1) values(53,53)",
                "insert into root.vehicle.d0(timestamp,s1) values(54,54)",
                "insert into root.vehicle.d0(timestamp,s1) values(55,55)",
                "insert into root.vehicle.d0(timestamp,s1) values(56,56)",
                "insert into root.vehicle.d0(timestamp,s1) values(57,57)",
                "insert into root.vehicle.d0(timestamp,s1) values(58,58)",

                "insert into root.vehicle.d0(timestamp,s2) values(1,7.0)",
                "insert into root.vehicle.d0(timestamp,s2) values(2,8.0)",
                "insert into root.vehicle.d0(timestamp,s2) values(100,19.0)",
                "insert into root.vehicle.d0(timestamp,s2) values(101,10.0)",
                "insert into root.vehicle.d0(timestamp,s2) values(102,18.0)",
                "insert into root.vehicle.d0(timestamp,s2) values(103,12.0)",
                "insert into root.vehicle.d0(timestamp,s2) values(104,13.0)",
                "insert into root.vehicle.d0(timestamp,s2) values(105,14.0)",
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.execute(sql);
            }
            statement.close();

            statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0,s1,s2 from root.vehicle.d0 where (time < 104 and s0 < 99) or (s2 < 16.0) or (s0 = 300)");
            // boolean hasResultSet = statement.execute("select s1 from root.vehicle.d0 where time < 104 and (s0 < 99 or s2 < 16.0)");
            // boolean hasResultSet = statement.execute("select s1 from root.vehicle.d0 where time < 104 and s0 < 99 and s2 < 16.0");
            Assert.assertTrue(hasResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1) + "," + resultSet.getString(d0s2);
                //System.out.println("====" + ans);
                Assert.assertEquals(retArray[cnt], ans);
                cnt++;
            }
            //Assert.assertEquals(cnt, 8);
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

    private void textDataTypeTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "101,199,null,tomorrow is another day",
                "102,180,10.0,tomorrow is another day",
                "946684800000,100,null,good"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            boolean hasTextMaxResultSet = statement.execute("select s1,s2,s3 from root.vehicle.d0 where s3 = 'tomorrow is another day' or s3 = 'good'");
            Assert.assertTrue(hasTextMaxResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s1) + "," +
                        resultSet.getString(d0s2) + "," + resultSet.getString(d0s3);
                //System.out.println("=====" + ans);
                Assert.assertEquals(ans, retArray[cnt++]);
            }
            Assert.assertEquals(cnt, 3);
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
