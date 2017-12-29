package cn.edu.tsinghua.iotdb.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class CompleteTest {

    private static final String TIMESTAMP_STR = "Time";

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
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
    public void Test() throws  ClassNotFoundException, SQLException {
        String[] sqls = {"SET STORAGE GROUP TO root.vehicle"};
        executeSQL(sqls);
        SimpleTest();
        InsertTest();
        UpdateTest();
        DeleteTest();
        SelectTest();
        FuncTest();
        GroupByTest();
    }

    public void SimpleTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
                "SHOW TIMESERIES",
                "===  Timeseries Tree  ===\n" +
                    "\n" +
                    "root:{\n" +
                    "    vehicle:{\n" +
                    "        d0:{\n" +
                    "            s0:{\n" +
                    "                 DataType: INT32,\n" +
                    "                 Encoding: RLE,\n" +
                    "                 args: {},\n" +
                    "                 StorageGroup: root.vehicle \n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}",
                "DELETE TIMESERIES root.vehicle.d0.s0",
                "SHOW TIMESERIES",
                "===  Timeseries Tree  ===\n" +
                    "\n" +
                    "root:{\n" +
                    "    vehicle\n" +
                    "}",
                "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=BOOLEAN,ENCODING=PLAIN",
                "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64,ENCODING=TS_2DIFF",
                "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT,ENCODING=GORILLA",
                "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=DOUBLE,ENCODING=RLE",
                "CREATE TIMESERIES root.vehicle.d1.s5 WITH DATATYPE=TEXT,ENCODING=PLAIN",
                "CREATE TIMESERIES root.vehicle.d2.s6 WITH DATATYPE=INT32,ENCODING=TS_2DIFF,COMPRESSOR=UNCOMPRESSOR",
                "CREATE TIMESERIES root.vehicle.d3.s7 WITH DATATYPE=INT32,ENCODING=RLE,COMPRESSOR=SNAPPY",
                "CREATE TIMESERIES root.vehicle.d4.s8 WITH DATATYPE=INT32,ENCODING=RLE,MAX_POINT_NUMBER=100",
                "CREATE TIMESERIES root.vehicle.d5.s9 WITH DATATYPE=FLOAT,ENCODING=PLAIN,COMPRESSOR=SNAPPY,MAX_POINT_NUMBER=10",
                "CREATE TIMESERIES root.vehicle.d6.s10 WITH DATATYPE=DOUBLE,ENCODING=RLE,COMPRESSOR=UNCOMPRESSOR,MAX_POINT_NUMBER=10",
                "DELETE TIMESERIES root.vehicle.d0.*",
                "SHOW TIMESERIES",
                "===  Timeseries Tree  ===\n" +
                        "\n" +
                        "root:{\n" +
                        "    vehicle:{\n" +
                        "        d1:{\n" +
                        "            s5:{\n" +
                        "                 DataType: TEXT,\n" +
                        "                 Encoding: PLAIN,\n" +
                        "                 args: {},\n" +
                        "                 StorageGroup: root.vehicle \n" +
                        "            }\n" +
                        "        },\n" +
                        "        d2:{\n" +
                        "            s6:{\n" +
                        "                 DataType: INT32,\n" +
                        "                 Encoding: TS_2DIFF,\n" +
                        "                 args: {COMPRESSOR=UNCOMPRESSOR},\n" +
                        "                 StorageGroup: root.vehicle \n" +
                        "            }\n" +
                        "        },\n" +
                        "        d3:{\n" +
                        "            s7:{\n" +
                        "                 DataType: INT32,\n" +
                        "                 Encoding: RLE,\n" +
                        "                 args: {COMPRESSOR=SNAPPY},\n" +
                        "                 StorageGroup: root.vehicle \n" +
                        "            }\n" +
                        "        },\n" +
                        "        d4:{\n" +
                        "            s8:{\n" +
                        "                 DataType: INT32,\n" +
                        "                 Encoding: RLE,\n" +
                        "                 args: {MAX_POINT_NUMBER=100},\n" +
                        "                 StorageGroup: root.vehicle \n" +
                        "            }\n" +
                        "        },\n" +
                        "        d5:{\n" +
                        "            s9:{\n" +
                        "                 DataType: FLOAT,\n" +
                        "                 Encoding: PLAIN,\n" +
                        "                 args: {COMPRESSOR=SNAPPY, MAX_POINT_NUMBER=10},\n" +
                        "                 StorageGroup: root.vehicle \n" +
                        "            }\n" +
                        "        },\n" +
                        "        d6:{\n" +
                        "            s10:{\n" +
                        "                 DataType: DOUBLE,\n" +
                        "                 Encoding: RLE,\n" +
                        "                 args: {COMPRESSOR=UNCOMPRESSOR, MAX_POINT_NUMBER=10},\n" +
                        "                 StorageGroup: root.vehicle \n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}",
                "DELETE TIMESERIES root.vehicle.*",
                "SHOW TIMESERIES",
                "===  Timeseries Tree  ===\n" +
                        "\n" +
                        "root:{\n" +
                        "    vehicle\n" +
                        "}"};
        executeSQL(sqlS);
    }

    public void InsertTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,101)",
                "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s0,s1) values(2,102,202)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(NOW(),104)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(2000-01-01T08:00:00+08:00,105)",
                "SELECT * FROM root.vehicle.d0",
                "1,101,null,\n" +
                        "2,102,202,\n" +
                        "946684800000,105,null,\n" +
                        "NOW(),104,null,\n",
                "DELETE TIMESERIES root.vehicle.*"};
        executeSQL(sqlS);
    }

    public void UpdateTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,1)",
                "UPDATE root.vehicle.d0 SET s0 = 2 WHERE time <= 2",
                "UPDATE root.vehicle SET d0.s0 = 3 WHERE time >= 9",
                "UPDATE root.vehicle.d0 SET s0 = 4 WHERE time <= 7 and time >= 5",
                "SELECT * FROM root.vehicle.d0",
                "1,2,\n" +
                        "2,2,\n" +
                        "3,1,\n" +
                        "4,1,\n" +
                        "5,4,\n" +
                        "6,4,\n" +
                        "7,4,\n" +
                        "8,1,\n" +
                        "9,3,\n" +
                        "10,3,\n",
                "DELETE TIMESERIES root.vehicle.*"};
        executeSQL(sqlS);
    }

    public void DeleteTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,1)",
                "SELECT * FROM root.vehicle.d0",
                "1,1,\n" +
                        "2,1,\n" +
                        "3,1,\n" +
                        "4,1,\n" +
                        "5,1,\n" +
                        "6,1,\n" +
                        "7,1,\n" +
                        "8,1,\n" +
                        "9,1,\n" +
                        "10,1,\n",
                "DELETE FROM root.vehicle.d0.s0 WHERE time < 8",
                "SELECT * FROM root.vehicle.d0",
                "8,1,\n" +
                        "9,1,\n" +
                        "10,1,\n",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(2000-01-01T08:00:00+08:00,1)",
                "SELECT * FROM root.vehicle.d0",
                "8,1,\n" +
                        "9,1,\n" +
                        "10,1,\n" +
                        "946684800000,1,\n",
                "DELETE FROM root.vehicle.d0.s0 WHERE time < 2000-01-02T08:00:00+08:00",
                "SELECT * FROM root.vehicle.d0",
                "",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(NOW(),1)",
                "SELECT * FROM root.vehicle.d0",
                "NOW(),1,\n",
                "DELETE FROM root.vehicle.d0.s0 WHERE time <= NOW()",
                "SELECT * FROM root.vehicle.d0",
                "",
                "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,1)",
                "INSERT INTO root.vehicle.d1(timestamp,s1) values(1,1)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,5)",
                "INSERT INTO root.vehicle.d1(timestamp,s1) values(5,5)",
                "SELECT * FROM root.vehicle",
                "1,1,1,\n" +
                        "5,5,5,\n",
                "DELETE FROM root.vehicle.d0.s0,root.vehicle.d1.s1 WHERE time < 3",
                "SELECT * FROM root.vehicle",
                "5,5,5,\n",
                "DELETE FROM root.vehicle.* WHERE time < 7",
                "SELECT * FROM root.vehicle",
                "",
                "DELETE TIMESERIES root.vehicle.*"};
        executeSQL(sqlS);
    }

    public void SelectTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,101)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,102)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,103)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,104)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,105)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,106)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,107)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,108)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,109)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,110)",
                "SELECT * FROM root.vehicle.d0 WHERE s0 < 104",
                "1,101,\n" +
                        "2,102,\n" +
                        "3,103,\n",
                "SELECT * FROM root.vehicle.d0 WHERE s0 > 105 and time < 8",
                "6,106,\n" +
                        "7,107,\n",
                "SELECT * FROM root.vehicle.d0",
                "1,101,\n" +
                        "2,102,\n" +
                        "3,103,\n" +
                        "4,104,\n" +
                        "5,105,\n" +
                        "6,106,\n" +
                        "7,107,\n" +
                        "8,108,\n" +
                        "9,109,\n" +
                        "10,110,\n",
                "DELETE TIMESERIES root.vehicle.*"};
        executeSQL(sqlS);
    }

    public void FuncTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,110)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,109)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,108)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,107)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,106)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,105)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,104)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,103)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,102)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,101)",
                "SELECT COUNT(s0) FROM root.vehicle.d0",
                "0,10,\n",
                "SELECT COUNT(s0) FROM root.vehicle.d0 WHERE root.vehicle.d0.s0 < 105",
                "0,4,\n",
                "SELECT MAX_TIME(s0) FROM root.vehicle.d0",
                "0,10,\n",
                "SELECT MAX_TIME(s0) FROM root.vehicle.d0 WHERE root.vehicle.d0.s0 > 105",
                "0,5,\n",
                "SELECT MIN_TIME(s0) FROM root.vehicle.d0",
                "0,1,\n",
                "SELECT MIN_TIME(s0) FROM root.vehicle.d0 WHERE root.vehicle.d0.s0 < 106",
                "0,6,\n",
                "SELECT MAX_VALUE(s0) FROM root.vehicle.d0",
                "0,110,\n",
                "SELECT MAX_VALUE(s0) FROM root.vehicle.d0 WHERE time > 4",
                "0,106,\n",
                "SELECT MIN_VALUE(s0) FROM root.vehicle.d0",
                "0,101,\n",
                "SELECT MIN_VALUE(s0) FROM root.vehicle.d0 WHERE time < 5",
                "0,107,\n",
                "DELETE FROM root.vehicle.d0.s0 WHERE time <= 10",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(NOW(),5)",
                "SELECT * FROM root.vehicle.d0",
                "NOW(),5,\n",
                "UPDATE root.vehicle.d0 SET s0 = 10 WHERE time <= NOW()",
                "SELECT * FROM root.vehicle.d0",
                "NOW(),10,\n",
                "DELETE FROM root.vehicle.d0.s0 WHERE time <= NOW()",
                "SELECT * FROM root.vehicle.d0",
                "",
                "DELETE TIMESERIES root.vehicle.*"};
        executeSQL(sqlS);
    }

    public void GroupByTest() throws ClassNotFoundException, SQLException {
        String[] sqlS = {
                "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,110)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,109)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,108)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,107)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,106)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,105)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,104)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,103)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,102)",
                "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,101)",
                "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT32,ENCODING=RLE",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(1,101)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(2,102)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(3,103)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(4,104)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(5,105)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(6,106)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(7,107)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(8,108)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(9,109)",
                "INSERT INTO root.vehicle.d0(timestamp,s1) values(10,110)",
                "SELECT COUNT(s0), COUNT(s1) FROM root.vehicle.d0 WHERE s1 < 109 GROUP BY(4ms,[1,10])",
                "1,3,3,\n" +
                        "4,4,4,\n" +
                        "8,1,1,\n",
                "SELECT COUNT(s0), MAX_VALUE(s1) FROM root.vehicle.d0 WHERE time < 7 GROUP BY(3ms,2,[1,5])",
                "1,1,101,\n" + "2,3,104,\n" + "5,1,105,\n",
                "SELECT MIN_VALUE(s0), MAX_TIME(s1) FROM root.vehicle.d0 WHERE s1 > 102 and time < 9 GROUP BY(3ms,1,[1,4],[6,9])",
                "1,108,3,\n" +
                        "4,105,6,\n" +
                        "7,103,8,\n",
                "DELETE TIMESERIES root.vehicle.*"};
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
                    result = data.toString();
                    cmp = true;
                } else {
                    if (sql.contains("NOW()") && now_start == 0L) {
                        now_start = System.currentTimeMillis();
                    }
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                    if (sql.split(" ")[0].equals("SELECT")) {
                        ResultSet resultSet = statement.getResultSet();
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int count = metaData.getColumnCount();
                        String[] column = new String[count];
                        for (int i = 0;i < count;i++) {
                            column[i] = metaData.getColumnName(i+1);
                        }
                        result = "";
                        while (resultSet.next()) {
                            for (int i = 1;i <= count;i++) {
                                if (now_start > 0L && column[i-1] == TIMESTAMP_STR) {
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
//                Assert.assertEquals();
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
