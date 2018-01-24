package cn.edu.tsinghua.iotdb.writelog;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.writelog.manager.MultiFileLogNodeManager;
import cn.edu.tsinghua.iotdb.writelog.node.ExclusiveWriteLogNode;
import cn.edu.tsinghua.iotdb.writelog.node.WriteLogNode;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class LogFileSizeTest {
    private IoTDB deamon;

    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
    private TSFileConfig fileConfig = TSFileDescriptor.getInstance().getConfig();

    private boolean skip = true;

    private int groupSize;
    private long runtime = 600000;

    private String[] setUpSqls = new String[]{
            "SET STORAGE GROUP TO root.logFileTest.bufferwrite",
            "SET STORAGE GROUP TO root.logFileTest.overflow",
            "CREATE TIMESERIES root.logFileTest.bufferwrite.val WITH DATATYPE=INT32, ENCODING=PLAIN",
            "CREATE TIMESERIES root.logFileTest.overflow.val WITH DATATYPE=INT32, ENCODING=PLAIN",
            // overflow baseline
            "INSERT INTO root.logFileTest.overflow(timestamp,val) VALUES (1000000000, 0)"
    };

    private String[] tearDownSqls = new String[] {
            "DELETE TIMESERIES root.logFileTest.*"
    };

    @Before
    public void setUp() throws Exception {
        if(skip)
            return;
        groupSize = fileConfig.groupSizeInByte;
        fileConfig.groupSizeInByte = 8 * 1024 * 1024;
        EnvironmentUtils.closeStatMonitor();
        deamon = IoTDB.getInstance();
        deamon.active();
        EnvironmentUtils.envSetUp();
        executeSQL(setUpSqls);
    }

    @After
    public void tearDown() throws Exception {
        if(skip)
            return;
        fileConfig.groupSizeInByte = groupSize;
        executeSQL(tearDownSqls);
        deamon.stop();
        Thread.sleep(5000);
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testBufferwrite() throws InterruptedException {
        if(skip)
            return;
        final long[] maxLength = {0};
        Thread writeThread = new Thread(() -> {
            int cnt = 0;
            try {
                Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return;
            }
            Connection connection = null;
            try {
                connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
                Statement statement = connection.createStatement();
                while(true) {
                    if(Thread.interrupted()){
                        System.out.println("Exit after " + cnt + " insertion");
                        break;
                    }
                    String sql = String.format("INSERT INTO root.logFileTest.bufferwrite(timestamp,val) VALUES (%d, %d)", ++cnt, cnt);
                    statement.execute(sql);
                    WriteLogNode logNode = MultiFileLogNodeManager.getInstance().getNode("root.logFileTest.bufferwrite" + TsFileDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX, null, null);
                    File bufferWriteWALFile = new File(logNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
                    if(bufferWriteWALFile.exists() && bufferWriteWALFile.length() > maxLength[0])
                        maxLength[0] = bufferWriteWALFile.length();
                }
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }
        });
        writeThread.start();
        Thread.sleep(runtime);
        writeThread.interrupt();
        while(writeThread.isAlive()) {

        }
        System.out.println("Max size of bufferwrite wal is " + MemUtils.bytesCntToStr(maxLength[0]) + " after " + runtime + "ms continuous writing");
    }

    @Test
    public void testOverflow() throws InterruptedException {
        if(skip)
            return;
        final long[] maxLength = {0};
        Thread writeThread = new Thread(() -> {
            int cnt = 0;
            try {
                Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return;
            }
            Connection connection = null;
            try {
                connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
                Statement statement = connection.createStatement();
                while(true) {
                    if(Thread.interrupted()){
                        System.out.println("Exit after " + cnt + " insertion");
                        break;
                    }
                    String sql = String.format("INSERT INTO root.logFileTest.overflow(timestamp,val) VALUES (%d, %d)", ++cnt, cnt);
                    statement.execute(sql);
                    WriteLogNode logNode = MultiFileLogNodeManager.getInstance().getNode("root.logFileTest.overflow" + TsFileDBConstant.OVERFLOW_LOG_NODE_SUFFIX, null, null);
                    File WALFile = new File(logNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
                    if(WALFile.exists() && WALFile.length() > maxLength[0])
                        maxLength[0] = WALFile.length();
                }
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }
        });
        writeThread.start();
        Thread.sleep(runtime);
        writeThread.interrupt();
        while(writeThread.isAlive()) {

        }
        System.out.println("Max size of overflow wal is " + MemUtils.bytesCntToStr(maxLength[0]) + " after " + runtime + "ms continuous writing");
    }

    private void executeSQL(String[] sqls) throws ClassNotFoundException, SQLException {
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
