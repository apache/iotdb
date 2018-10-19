package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileDatabaseMetadata;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class LimitSlimitTest {
    private static final String TIMESTAMP_STR = "Time";

    private static String[] insertSqls = new String[]{
            "SET STORAGE GROUP TO root.vehicle",

            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",

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

            "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
    };

    public static void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : insertSqls) {
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
    public void Test() throws ClassNotFoundException, SQLException {
        insertSQL();

        SelectTest();
        GroupByTest();
        FuncTest();
        FillTest();

    }

    public void SelectTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "SELECT s1 FROM root.vehicle.d0 WHERE time<200 limit 3",
                "1,1101,\n" +
                        "2,40000,\n" +
                        "50,50000,\n",

                "SELECT s0 FROM root.vehicle.d0 WHERE s1 > 190 limit 3",
                "1,null,\n" +
                        "2,null,\n" +
                        "50,null,\n",

                "SELECT s1,s2 FROM root.vehicle.d0 where s1>190 or s2<10.0 limit 3 offset 2",
                "3,null,3.33,\n" +
                        "4,null,4.44,\n" +
                        "50,50000,null,\n",

                "select * from root.vehicle.d0 slimit 1",
                "1,null,\n" +
                        "2,null,\n" +
                        "3,null,\n" +
                        "4,null,\n" +
                        "50,null,\n" +
                        "100,null,\n" +
                        "101,null,\n" +
                        "102,null,\n" +
                        "103,null,\n" +
                        "104,33333,\n" +
                        "105,33333,\n" +
                        "106,99,\n" +
                        "1000,22222,\n" +
                        "946684800000,null,\n",

                "select * from root.vehicle.d0 slimit 1 soffset 2",
                "1,null,\n" +
                        "2,2.22,\n" +
                        "3,3.33,\n" +
                        "4,4.44,\n" +
                        "50,null,\n" +
                        "100,null,\n" +
                        "101,null,\n" +
                        "102,10.0,\n" +
                        "103,null,\n" +
                        "104,null,\n" +
                        "105,11.11,\n" +
                        "106,null,\n" +
                        "1000,1000.11,\n" +
                        "946684800000,null,\n",

                "select d0 from root.vehicle slimit 1 soffset 2",
                "1,null,\n" +
                        "2,2.22,\n" +
                        "3,3.33,\n" +
                        "4,4.44,\n" +
                        "50,null,\n" +
                        "100,null,\n" +
                        "101,null,\n" +
                        "102,10.0,\n" +
                        "103,null,\n" +
                        "104,null,\n" +
                        "105,11.11,\n" +
                        "106,null,\n" +
                        "1000,1000.11,\n" +
                        "946684800000,null,\n",

                "select * from root.vehicle.d0 where s1>190 or s2 < 10.0 limit 3 offset 1 slimit 1 soffset 2 ",
                "2,2.22,\n" +
                        "3,3.33,\n" +
                        "4,4.44,\n"

        };
        executeSQL(sqlS);
    }

    // according to the current groupBy
    public void GroupByTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "select count(s1), count(s2) from root.vehicle.d0 where s1>190 or s2< 10.0 group by(10ms, 0, [3,10000]) limit 6",
                "3,null,2,\n" +
                        "10,null,null,\n" +
                        "20,null,null,\n" +
                        "30,null,null,\n" +
                        "40,null,null,\n" +
                        "50,1,null,\n",
                "select count(*) from root.vehicle.d0 where s1>190 or s2< 10.0 group by(10ms, 0, [3,10000]) slimit 1 soffset 1 limit 6",
                "3,null,\n" +
                        "10,null,\n" +
                        "20,null,\n" +
                        "30,null,\n" +
                        "40,null,\n" +
                        "50,1,\n",
                "select count(d0) from root.vehicle where root.vehicle.d0.s1>190 or root.vehicle.d0.s2< 10.0 group by(10ms, 0, [3,10000]) slimit 1 soffset 1 limit 6",
                "3,null,\n" +
                        "10,null,\n" +
                        "20,null,\n" +
                        "30,null,\n" +
                        "40,null,\n" +
                        "50,1,\n"
        };
        executeSQL(sqlS);
    }

    public void FuncTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "SELECT COUNT(s1) FROM root.vehicle.d0 WHERE time<200 limit 3",
                "0,9,\n",
                "SELECT max_time(s0) FROM root.vehicle.d0 WHERE s1 > 190 limit 3",
                "0,1000,\n",
                "SELECT max_value(s1),min_value(s2) FROM root.vehicle.d0 where s1>190 or s2<10.0 limit 3",
                "0,55555,2.22,\n"
        };
        executeSQL(sqlS);
    }

    public void FillTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "select * from root.vehicle.d0 where time=106 fill(float[previous,10m],int64[previous,10m],int32[previous,10m]) slimit 2 soffset 1",
                "106,199,11.11,\n"
        };
        executeSQL(sqlS);
    }

    private void executeSQL(String[] sqls) throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            String result = "";
            Long now_start = 0L;
            boolean cmp = false;
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            for (String sql : sqls) {
                if (cmp) {
                    Assert.assertEquals(result, sql);
                    cmp = false;
                } else if (sql.equals("SHOW TIMESERIES")) {
                    DatabaseMetaData data = connection.getMetaData();
                    result = ((TsfileDatabaseMetadata)data).getMetadataInJson();
                    cmp = true;
                } else {
                    if (sql.contains("NOW()") && now_start == 0L) {
                        now_start = System.currentTimeMillis();
                    }
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                    if (sql.split(" ")[0].equals("SELECT") | sql.split(" ")[0].equals("select")) {
                        ResultSet resultSet = statement.getResultSet();
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int count = metaData.getColumnCount();
                        String[] column = new String[count];
                        for (int i = 0; i < count; i++) {
                            column[i] = metaData.getColumnName(i + 1);
                        }
                        result = "";
                        while (resultSet.next()) {
                            for (int i = 1; i <= count; i++) {
                                if (now_start > 0L && column[i - 1] == TIMESTAMP_STR) {
                                    String timestr = resultSet.getString(i);
                                    Long tn = Long.valueOf(timestr);
                                    Long now = System.currentTimeMillis();
                                    if (tn >= now_start && tn <= now) {
                                        timestr = "NOW()";
                                    }
                                    result += timestr + ',';
                                } else {
                                    result += resultSet.getString(i) + ',';
                                }
                            }
                            result += '\n';
                        }
                        cmp = true;
                    }
                    statement.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
