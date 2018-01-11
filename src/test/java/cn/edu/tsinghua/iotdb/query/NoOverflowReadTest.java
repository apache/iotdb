package cn.edu.tsinghua.iotdb.query;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.query.engine.AggregateEngine;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.*;

import static org.junit.Assert.fail;

public class NoOverflowReadTest {
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

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    private int maxNumberOfPointsInPage;
    private int pageSizeInByte;
    private int groupSizeInByte;

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.closeStatMonitor();
        deamon = IoTDB.getInstance();
        deamon.active();
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        deamon.stop();
        Thread.sleep(5000);
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException, FileNotFoundException {


        Thread.sleep(5000);
        insertSQL();

        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

        selectWildTest();

        connection.close();

    }

    // to fix issue https://github.com/thulab/iotdb/issues/230
    // it is an error caused by IntervalTimeVisitor.satisfy method
    private void selectWildTest() throws ClassNotFoundException, SQLException {
        String selectSql = "select s0 from root.vehicle.d0";

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
                //String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0);
                //System.out.println("===" + ans);
                cnt++;
            }
            Assert.assertEquals(20900, cnt);
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
