package cn.edu.tsinghua.iotdb.service;

import java.io.File;
import java.sql.*;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Just used for integration test.
 */
public class DaemonTest {
    private final String FOLDER_HEADER = "src/test/resources";
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

    private String overflowDataDirPre;
    private String fileNodeDirPre;
    private String bufferWriteDirPre;
    private String metadataDirPre;
    private String derbyHomePre;

    private Daemon deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
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
            deamon = new Daemon();
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
    public void test() {
        if (testFlag) {
            try {
                Thread.sleep(5000);
                insertSQL();

                Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
                //System.out.println(connection.getMetaData());
                selectAllSQLTest();
                dnfErrorSQLTest();
                selectWildCardSQLTest();
                selectAndOperatorTest();
                selectAndOpeCrossTest();
                aggregationTest();
                selectOneColumnWithFilterTest();
                multiAggregationTest();
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
            if (hasResultSet) {
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
            }
            statement.close();

            // the statement has same columns and same aggregation
            retArray = new String[]{
                    "11,11,6,1000.11"
            };
            statement = connection.createStatement();
            hasResultSet = statement.execute("select count(s1),count(s1),count(s3),max_value(s2) from root.vehicle.d0");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
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
            }
            statement.close();

            // the statement has same columns and different aggregation
            retArray = new String[]{
                    "11,55555,946684800000,6"
            };
            statement = connection.createStatement();
            hasResultSet = statement.execute("select count(s1),max_value(s1),max_time(s1),count(s3) from root.vehicle.d0");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
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
            }
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
            // System.out.println(hasResultSet + "...");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                            +","+resultSet.getString(d0s2)+","+resultSet.getString(d0s3)+","+resultSet.getString(d1s0);
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
                    //System.out.println("======" + ans);
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
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1);
                    Assert.assertEquals(ans, retArray[cnt]);
                    cnt++;
                    // AbstractClient.output(resultSet, true, "select statement");
                }
                Assert.assertEquals(6, cnt);
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
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s2);
                    // System.out.println(ans);
                    Assert.assertEquals(ans, retArray[cnt]);
                    cnt++;
                    // AbstractClient.output(resultSet, true, "select statement");
                }
                Assert.assertEquals(6, cnt);
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
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0)+","+resultSet.getString(d0s1)+","
                            +resultSet.getString(d1s0);
                    // System.out.println(ans);
                    Assert.assertEquals(ans, retArray[cnt]);
                    cnt++;
                    // AbstractClient.output(resultSet, true, "select statement");
                }
                Assert.assertEquals(1,cnt);
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

    private void selectAndOpeCrossTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1000,22222,55555"};

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0,s1 from root.vehicle.d0 where time > 106 and root.vehicle.d1.s0 > 100");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0)+","+resultSet.getString(d0s1);
                    // System.out.println(ans);
                    Assert.assertEquals(ans, retArray[cnt]);
                    cnt++;
                    //AbstractClient.output(resultSet, true, "select statement");
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
            if (hasTextMaxResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(1);
                    // System.out.println("=====" + ans);
                    Assert.assertEquals(retArray[0], ans);
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
            statement.close();

            statement = connection.createStatement();
            boolean hasTextMinResultSet = statement.execute("select min_value(s3) from root.vehicle.d0");
            if (hasTextMinResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(1);
                    // System.out.println("=====" + ans);
                    Assert.assertEquals(ans, "aaaaa");
                    cnt++;
                }
                Assert.assertEquals(cnt, 1);
            }
            statement.close();

            statement = connection.createStatement();
            boolean hasMultiAggreResult = statement.execute("select min_value(s0) from root.vehicle.d0,root.vehicle.d1");
            if (hasMultiAggreResult) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    int ans1 = resultSet.getInt(1);
                    int ans2 = resultSet.getInt(2);
                    Assert.assertEquals(ans1, 99);
                    Assert.assertEquals(ans2, 888);
                    cnt++;
                }
                Assert.assertEquals(cnt, 1);
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
            if (hasTextMaxResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s1);
                    //System.out.println("=====" + ans);
                    Assert.assertEquals(ans, retArray[cnt++]);
                    //AbstractClient.output(resultSet, true, "select statement");
                }
                Assert.assertEquals(cnt, 3);
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
            if (hasTextMaxResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s1) + "," +
                            resultSet.getString(d0s2) + "," + resultSet.getString(d0s3);
                    //System.out.println("=====" + ans);
                    Assert.assertEquals(ans, retArray[cnt++]);
                }
                Assert.assertEquals(cnt, 3);
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
