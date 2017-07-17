package cn.edu.thu.tsfiledb.service;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.*;

import cn.edu.thu.tsfiledb.jdbc.AbstractClient;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

/**
 * Just used for integration test.
 */
public class DaemonTest {
    private final String FOLDER_HEADER = "src/test/resources";
    private String[] sqls = new String[]{
            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=BYTE_ARRAY, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "SET STORAGE GROUP TO root.vehicle",

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
            "UPDATE root.vehicle.d0.s0 SET VALUE = 33333 WHERE time < 106 and time > 103",

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
            "UPDATE root.vehicle.d0.s3 SET VALUE = 'tomorrow is another day' WHERE time >100 and time < 103",

            "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
            "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",

            "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
            "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",
            "insert into root.vehicle.d0(timestamp,s2) values(now(),55.5)",
};

    private String overflowDataDirPre;
    private String fileNodeDirPre;
    private String bufferWriteDirPre;
    private String metadataDirPre;
    private String derbyHomePre;

    private Daemon deamon;

    @Before
    public void setUp() throws Exception {
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

    @After
    public void tearDown() throws Exception {
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

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException {
        Thread.sleep(5000);
        insertSQL();


        //TODO: add your query statement
        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        System.out.println(connection.getMetaData());
        // outSQL();
        dnfErrorSQL();
        connection.close();
    }

    private void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
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

    private void outSQL() throws ClassNotFoundException, SQLException {
        Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select * from root");
            System.out.println(hasResultSet + "...");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                while (resultSet.next()) {
                    AbstractClient.output(resultSet, true, "select *");
                }
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

    private void dnfErrorSQL() throws ClassNotFoundException, SQLException {
        Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select s0,s1 from root.vehicle.d0 where time < 106 and (s0 >= 60 or s1 <= 200)");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                while (resultSet.next()) {
                    AbstractClient.output(resultSet, true, "select *");
                }
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
