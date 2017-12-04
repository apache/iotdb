package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.query.engine.AggregateEngine;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.*;

import static cn.edu.tsinghua.iotdb.service.TestUtils.count;
import static cn.edu.tsinghua.iotdb.service.TestUtils.max_value;
import static cn.edu.tsinghua.iotdb.service.TestUtils.min_value;
import static cn.edu.tsinghua.iotdb.service.TestUtils.max_time;
import static cn.edu.tsinghua.iotdb.service.TestUtils.min_time;
import static org.junit.Assert.fail;

/**
 * This junit test is used for ```Group By``` function test.
 * <p>
 * Notice that: to make sure that the batch read in ```Group By``` process is collect,
 * (1) the fetchSize parameter in method <code>queryOnePath()</code> in <code>GroupByEngineNoFilter</code> should
 * be set very small
 * (2) the aggregateFetchSize parameter in class <code>GroupByEngineWithFilter</code> should
 * be set very small
 */
public class GroupBySmallDataTest {
    private final String FOLDER_HEADER = "src/test/resources";
    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d0s4 = "root.vehicle.d0.s4";
    private final String d1s0 = "root.vehicle.d1.s0";
    private final String d1s1 = "root.vehicle.d1.s1";

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
            "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",

            "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
            "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
    };

    private String overflowDataDirPre;
    private String fileNodeDirPre;
    private String bufferWriteDirPre;
    private String metadataDirPre;
    private String derbyHomePre;

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            AggregateEngine.aggregateFetchSize = 4000;
            TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
            overflowDataDirPre = config.overflowDataDir;
            fileNodeDirPre = config.fileNodeDir;
            bufferWriteDirPre = config.bufferWriteDir;
            metadataDirPre = config.metadataDir;
            derbyHomePre = config.derbyHome;

            config.overflowDataDir = FOLDER_HEADER + "/data/overflow";
            config.fileNodeDir = FOLDER_HEADER + "/data/digest";
            config.bufferWriteDir = FOLDER_HEADER + "/data/delta";
            config.metadataDir = FOLDER_HEADER + "/data/metadata";
            config.derbyHome = FOLDER_HEADER + "/data/derby";
            deamon = new IoTDB();
            deamon.active();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(5000);

            TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
            FileUtils.deleteDirectory(new File(config.overflowDataDir));
            FileUtils.deleteDirectory(new File(config.fileNodeDir));
            FileUtils.deleteDirectory(new File(config.bufferWriteDir));
            FileUtils.deleteDirectory(new File(config.metadataDir));
            FileUtils.deleteDirectory(new File(config.derbyHome));
            FileUtils.deleteDirectory(new File(FOLDER_HEADER + "/data"));

            config.overflowDataDir = overflowDataDirPre;
            config.fileNodeDir = fileNodeDirPre;
            config.bufferWriteDir = bufferWriteDirPre;
            config.metadataDir = metadataDirPre;
            config.derbyHome = derbyHomePre;
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException {
        if (testFlag) {
            Thread.sleep(5000);
            MultiAggreWithFilterTest.insertSQL();

            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            // selectAllSQLTest();
            groupByNoFilterOneIntervalTest();
            groupByWithFilterCountOneIntervalTest();
            groupByWithFilterMaxMinValueOneIntervalTest();
            groupByWithFilterMaxTimeOneIntervalTest();
            groupByWithFilterMinTimeOneIntervalTest();
            groupByNoValidIntervalTest();
            groupByMultiResultWithFilterTest();
            groupByWithFilterCountManyIntervalTest();
            threadLocalTest();
            groupByMultiAggregationFunctionTest();
            groupBySelectMultiDeltaObjectTest();


            // no need to test for output too much

            // groupByMultiResultNoFilterTest();
            // bugSelectClauseTest();

            connection.close();
        }
    }

    private void groupByWithFilterCountOneIntervalTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),count(s1),count(s2),count(s3) " +
                    "from root.vehicle.d0 where s1 > 190 or s2 < 10.0 group by(10ms, 0, [3,10000])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                            + "," + resultSet.getString(count(d0s3));
                    //System.out.println(ans);
                    switch (cnt) {
                        case 1:
                            Assert.assertEquals("3,null,null,2,null", ans);
                            break;
                        case 6:
                            Assert.assertEquals("50,null,1,null,null", ans);
                            break;
                        case 11:
                            Assert.assertEquals("100,1,4,1,1", ans);
                            break;
                        case 101:
                            Assert.assertEquals("1000,1,1,1,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(1002, cnt);
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

    private void groupByWithFilterMaxMinValueOneIntervalTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet;

            hasResultSet = statement.execute("select max_value(s0),max_value(s1),max_value(s2),max_value(s3) " +
                    "from root.vehicle.d0 where s1 > 190 or s2 < 10.0 group by(10ms, 0, [3,10000])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d0s0))
                            + "," + resultSet.getString(max_value(d0s1)) + "," + resultSet.getString(max_value(d0s2))
                            + "," + resultSet.getString(max_value(d0s3));
                    // System.out.println(ans);
                    switch (cnt) {
                        case 1:
                            Assert.assertEquals("3,null,null,4.44,null", ans);
                            break;
                        case 6:
                            Assert.assertEquals("50,null,50000,null,null", ans);
                            break;
                        case 11:
                            Assert.assertEquals("100,33333,199,11.11,tomorrow is another day", ans);
                            break;
                        case 101:
                            Assert.assertEquals("1000,22222,55555,1000.11,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(1002, cnt);
            }

            statement.close();

            statement = connection.createStatement();
            hasResultSet = statement.execute("select min_value(s0),min_value(s1),min_value(s2),min_value(s3) " +
                    "from root.vehicle.d0 where s1 > 190 or s2 < 10.0 group by(10ms, 0, [3,10000])");
            Assert.assertTrue(hasResultSet);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(min_value(d0s0))
                            + "," + resultSet.getString(min_value(d0s1)) + "," + resultSet.getString(min_value(d0s2))
                            + "," + resultSet.getString(min_value(d0s3));
                    // System.out.println(ans);
                    switch (cnt) {
                        case 1:
                            Assert.assertEquals("3,null,null,3.33,null", ans);
                            break;
                        case 6:
                            Assert.assertEquals("50,null,50000,null,null", ans);
                            break;
                        case 11:
                            Assert.assertEquals("100,33333,199,11.11,tomorrow is another day", ans);
                            break;
                        case 101:
                            Assert.assertEquals("1000,22222,55555,1000.11,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(1002, cnt);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void groupByWithFilterMaxTimeOneIntervalTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet;

            hasResultSet = statement.execute("select max_time(s0),max_time(s1),max_time(s2),max_time(s3) " +
                    "from root.vehicle.d0 where s1 > 190 or s2 < 10.0 group by(10ms, 0, [3,10000])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_time(d0s0))
                            + "," + resultSet.getString(max_time(d0s1)) + "," + resultSet.getString(max_time(d0s2))
                            + "," + resultSet.getString(max_time(d0s3));
                    // System.out.println(ans);
                    switch (cnt) {
                        case 1:
                            Assert.assertEquals("3,null,null,4,null", ans);
                            break;
                        case 6:
                            Assert.assertEquals("50,null,50,null,null", ans);
                            break;
                        case 11:
                            Assert.assertEquals("100,105,105,105,101", ans);
                            break;
                        case 101:
                            Assert.assertEquals("1000,1000,1000,1000,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(1002, cnt);
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

    private void groupByWithFilterMinTimeOneIntervalTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select min_time(s0),min_time(s1),min_time(s2),min_time(s3) " +
                    "from root.vehicle.d0 where s1 > 190 or s2 < 10.0 group by(10ms, 0, [3,10000])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(min_time(d0s0))
                            + "," + resultSet.getString(min_time(d0s1)) + "," + resultSet.getString(min_time(d0s2))
                            + "," + resultSet.getString(min_time(d0s3));
                    // System.out.println(ans);
                    switch (cnt) {
                        case 1:
                            Assert.assertEquals("3,null,null,3,null", ans);
                            break;
                        case 6:
                            Assert.assertEquals("50,null,50,null,null", ans);
                            break;
                        case 11:
                            Assert.assertEquals("100,105,100,105,101", ans);
                            break;
                        case 101:
                            Assert.assertEquals("1000,1000,1000,1000,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(1002, cnt);
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

    private void groupByNoFilterOneIntervalTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),count(s1),count(s2),count(s3) " +
                    "from root.vehicle.d0 group by(10ms, 0, [3,10000])");

            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                            + "," + resultSet.getString(count(d0s3));
                    //System.out.println(ans);
                    switch (cnt) {
                        case 1:
                            Assert.assertEquals("3,null,null,2,null", ans);
                            break;
                        case 6:
                            Assert.assertEquals("50,null,1,null,null", ans);
                            break;
                        case 7:
                        case 8:
                        case 9:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,1", ans);
                            break;
                        case 11:
                            Assert.assertEquals("100,3,6,2,2", ans);
                            break;
                        case 101:
                            Assert.assertEquals("1000,1,1,1,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(1002, cnt);
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

    private void groupByMultiAggregationFunctionTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),min_value(s1),max_value(s2),min_time(s3) " +
                    "from root.vehicle.d0 where s1 > 190 or s2 < 10.0 group by(10ms, 0, [3,103], [998,1002])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(min_value(d0s1)) + "," + resultSet.getString(max_value(d0s2))
                            + "," + resultSet.getString(min_time(d0s3));
                    // System.out.println(ans);
                    switch (cnt) {
                        case 1:
                            Assert.assertEquals("3,null,null,4.44,null", ans);
                            break;
                        case 6:
                            Assert.assertEquals("50,null,50000,null,null", ans);
                            break;
                        case 11:
                            Assert.assertEquals("100,null,199,null,101", ans);
                            break;
                        case 13:
                            Assert.assertEquals("1000,1,55555,1000.11,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(14, cnt);
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

    private void groupByNoValidIntervalTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),min_value(s1),max_value(s2),min_time(s3) " +
                    "from root.vehicle.d0 group by(10ms, 0, [300,103], [998,1002])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(min_value(d0s1)) + "," + resultSet.getString(max_value(d0s2))
                            + "," + resultSet.getString(min_time(d0s3));
                    // System.out.println(ans);
                    switch (cnt) {
                        case 2:
                            Assert.assertEquals("1000,1,55555,1000.11,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(3, cnt);
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

    private void groupByMultiResultNoFilterTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
//            boolean hasResultSet = statement.execute("select count(s0),min_value(s1),max_value(s2),min_time(s3) " +
//                    "from root.vehicle.d0 group by(1ms, 0, [0,10000000])");

            String sql = "select count(s0),min_value(s0),max_value(s0),min_time(s0) from root.vehicle.d0 group by(10ms, 0, [2010-01-01T00:00:00.000,2010-01-08T16:43:15.000])";
            boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    cnt++;
                }
                System.out.println("--------" + cnt);
                //Assert.assertEquals(10000002, cnt);
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

    private void groupByMultiResultWithFilterTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),min_value(s1),max_value(s2),min_time(s3) " +
                    "from root.vehicle.d0 where s0 != 0 group by(1ms, 0, [0,10000000])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    cnt++;
                }
                Assert.assertEquals(10000002, cnt);
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

    private void groupBySelectMultiDeltaObjectTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),min_value(s1),max_value(s2),min_time(s3) " +
                    "from root.vehicle.d0,root.vehicle.d1 group by(100ms, 0, [0,1500])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(min_value(d0s1)) + "," + resultSet.getString(max_value(d0s2))
                            + "," + resultSet.getString(min_time(d0s3)) + "," + resultSet.getString(min_time(d0s3));
                    System.out.println(ans);

                    cnt++;
                }
                Assert.assertEquals(17, cnt);
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

    private void threadLocalTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),min_value(s1)" +
                    "from root.vehicle.d0 group by(100ms, 0, [0,1500])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(min_value(d0s1));
                    System.out.println(ans);
                    cnt++;
                }
            }
            statement.close();

            statement = connection.createStatement();
            hasResultSet = statement.execute("select count(s0),min_value(s1)" +
                    "from root.vehicle.d0 group by(10ms, 0, [1600,1700])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(min_value(d0s1)) + "," + resultSet.getString(min_value(d0s1));
                    System.out.println(ans);
                    cnt++;
                }
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

    // no need to test for output too much

    private void groupByWithFilterCountManyIntervalTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select count(s0),count(s1),count(s2),count(s3) " +
                    "from root.vehicle.d0 where s1 > 190 or s2 < 10.0 group by(10ms, 0, [3,103], [998,1002])");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                            + "," + resultSet.getString(count(d0s3));
                    //System.out.println(ans);
                    switch (cnt) {
                        case 1:
                            Assert.assertEquals("3,null,null,2,null", ans);
                            break;
                        case 6:
                            Assert.assertEquals("50,null,1,null,null", ans);
                            break;
                        case 11:
                            Assert.assertEquals("100,null,3,null,1", ans);
                            break;
                        case 13:
                            Assert.assertEquals("1000,1,1,1,null", ans);
                            break;
                        default:
                            Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null", ans);
                    }
                    cnt++;
                }
                Assert.assertEquals(14, cnt);
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

    private void bugSelectClauseTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0 " +
                    "from root.vehicle.d0 where (time > 0) or (time < 2000) and (s1 > 0)");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                            + "," + resultSet.getString(min_value(d0s1)) + "," + resultSet.getString(max_value(d0s2))
                            + "," + resultSet.getString(min_time(d0s3)) + "," + resultSet.getString(min_time(d0s3));
                    System.out.println(ans);

                    cnt++;
                }
                Assert.assertEquals(17, cnt);
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
