package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class TsFilesReaderTest {
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

    private IoTDB daemon;
    private boolean testFlag = TestUtils.testFlag;

    private Map<String, Integer> seriesName2mod = new HashMap<String, Integer>();

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            daemon = IoTDB.getInstance();
            daemon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            daemon.stop();
            Thread.sleep(5000);
            EnvironmentUtils.cleanEnv();
        }
    }


    public void insertData() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            //create storage group and measurement
            for(String sql : create_sql){
                statement.execute(sql);
            }

            //insert data (time from 300-1000)
            for(long time = 300; time < 1001; time++){
                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[(int)time % 5]);
                statement.execute(sql);
            }

            statement.execute("flush");

            //insert data (time from 1200-1500)
            for(long time = 1200; time < 1501; time++){
                String sql = null;
                if(time % 2 == 0){
                    sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
                    statement.execute(sql);
                    sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
                    statement.execute(sql);
                }
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[(int)time % 5]);
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



    @Test
    public void TsFilesReaderWithoutFilterTest(){

    }
    @Test
    public void TsFilesReaderWithFilterTest(){

    }
    @Test
    public void TsFilesReaderWithTimeStampTest(){

    }

    @After
    public void clearUp(){

    }

}
