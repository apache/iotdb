package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.IntDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.StringDataPoint;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static junit.framework.TestCase.assertEquals;

@Deprecated
public class IoTDBMemControlTest {
    private final String FOLDER_HEADER = "src/test/resources";
    private static final String TIMESTAMP_STR = "Time";
    private final String d0 = "root.vehicle.d0";
    private final String d1 = "root.house.d0";
    private final String s0 = "s0";
    private final String s1 = "s1";
    private final String s2 = "s2";
    private final String s3 = "s3";

    private String[] sqls = new String[]{
            "SET STORAGE GROUP TO root.vehicle",
            "SET STORAGE GROUP TO root.house",
            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
            
            "CREATE TIMESERIES root.house.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.house.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.house.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.house.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.house.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.house.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
    };

    TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
    private IoTDB deamon;

    private boolean testFlag = false;
    private boolean exceptionCaught = false;

    public IoTDBMemControlTest() {
    }

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            deamon = IoTDB.getInstance();
            

            config.memThresholdWarning = 3 * TsFileDBConstant.MB;
            config.memThresholdDangerous = 5 * TsFileDBConstant.MB;

            BasicMemController.getInstance().setCheckInterval(15 * 1000);
            BasicMemController.getInstance().setDangerouseThreshold(config.memThresholdDangerous);  // force initialize
            BasicMemController.getInstance().setWarningThreshold(config.memThresholdWarning);

            deamon.active();
            EnvironmentUtils.envSetUp();
            insertSQL();
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
        // test a huge amount of write causes block
        if(!testFlag)
            return;
        Thread t1 = new Thread(() -> insert(d0));
        Thread t2 = new Thread(() -> insert(d1));
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        assertEquals(exceptionCaught, true);
        assertEquals(BasicMemController.UsageLevel.WARNING, BasicMemController.getInstance().getCurrLevel());

        // test MemControlTread auto flush
        Thread.sleep(15000);
        assertEquals(BasicMemController.UsageLevel.SAFE, BasicMemController.getInstance().getCurrLevel());
    }

    public void insert(String deviceId) {
        try {
            Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = null;
        TSRecord record = new TSRecord(0, deviceId);
        record.addTuple(new IntDataPoint(s0, 0));
        record.addTuple(new LongDataPoint(s1, 0));
        record.addTuple(new FloatDataPoint(s2, 0.0f));
        record.addTuple(new StringDataPoint(s3, new Binary("\"sadasgagfdhdshdhdfhdfhdhdhdfherherdfsdfbdfsherhedfjerjerdfshfdshxzcvenerhreherjnfdgntrnt" +
                "ddfhdsf,joreinmoidnfh\"")));
        long recordMemSize = MemUtils.getTsRecordMemBufferwrite(record);
        long insertCnt = config.memThresholdDangerous / recordMemSize * 2;
        System.out.println(Thread.currentThread().getId() + " to insert " + insertCnt);
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (int i = 0; i < insertCnt; i++) {
                record.time = i + 1;
                statement.execute(TestUtils.recordToInsert(record));
                if(i % 1000 == 0) {
                    System.out.println(Thread.currentThread().getId() + " inserting " + i);
                }
            }
            statement.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if(e.getMessage().contains("exceeded dangerous threshold"))
                exceptionCaught = true;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
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
}
