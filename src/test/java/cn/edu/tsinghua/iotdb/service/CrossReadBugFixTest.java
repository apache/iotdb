package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.*;

/**
 *
 */
public class CrossReadBugFixTest {
    private final String FOLDER_HEADER = "src/test/resources";
    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d1s0 = "root.vehicle.d1.s0";
    private final String d1s1 = "root.vehicle.d1.s1";

    private String[] sqls = new String[]{
            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "SET STORAGE GROUP TO root.vehicle",

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

            // "merge",

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

    private String overflowDataDirPre;
    private String fileNodeDirPre;
    private String bufferWriteDirPre;
    private String metadataDirPre;
    private String derbyHomePre;

    private Daemon deamon;

    //@Before
    public void setUp() throws Exception {
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        overflowDataDirPre = config.overflowDataDir;
        fileNodeDirPre = config.fileNodeDir;
        bufferWriteDirPre = config.bufferWriteDir;
        metadataDirPre = config.metadataDir;
        derbyHomePre = config.derbyHome;

        TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
        tsFileConfig.maxNumberOfPointsInPage = 4;
        tsFileConfig.pageSizeInByte = 300;

        config.overflowDataDir = FOLDER_HEADER + "/data/overflow";
        config.fileNodeDir = FOLDER_HEADER + "/data/digest";
        config.bufferWriteDir = FOLDER_HEADER + "/data/delta";
        config.metadataDir = FOLDER_HEADER + "/data/metadata";
        config.derbyHome = FOLDER_HEADER + "/data/derby";
        deamon = new Daemon();
        deamon.active();
    }

    //@After
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

    //@Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException {
        Thread.sleep(5000);
        insertSQL();

        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        System.out.println(connection.getMetaData());
        selectWildTest();
        crossReadTest();
        connection.close();
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

    private void selectWildTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1,101,1101,7.0",
                "2,198,198,8.0",
                "51,null,51,null",
                "52,null,52,null",
                "53,null,53,null",
                "54,null,54,null",
                "55,null,55,null",
                "56,null,56,null",
                "57,null,57,null",
                "58,null,58,null",
                "100,300,199,19.0",
                "101,99,199,10.0",
                "102,80,180,18.0",
                "103,99,199,12.0",
                "104,90,190,13.0",
                "105,99,199,14.0",
                "106,99,null,null"
        };

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            boolean hasResultSet = statement.execute("select * from root");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + ","
                            + resultSet.getString(d0s1) + "," + resultSet.getString(d0s2);
                    // System.out.println(ans);
                    Assert.assertEquals(retArray[cnt], ans);
                    cnt++;
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

    /**
     *  <p>Insert bufferwrite, merge, and insert overflow.
     *  Cross read.
     *
     */
    private void crossReadTest() throws ClassNotFoundException, SQLException {
        String[] retArray = new String[]{
                "1,101,1101,7.0",
                "2,198,198,8.0",
                "100,300,199,19.0",
                "101,99,199,10.0",
                "102,80,180,18.0",
                "103,99,199,12.0",
                "104,90,190,13.0",
                "105,99,199,14.0"};

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            boolean hasResultSet = statement.execute("select s0,s1,s2 from root.vehicle.d0 where (time < 104 and s0 < 99) or (s2 < 16.0) or (s0 = 300)");
            // boolean hasResultSet = statement.execute("select s1 from root.vehicle.d0 where time < 104 and (s0 < 99 or s2 < 16.0)");
            // boolean hasResultSet = statement.execute("select s1 from root.vehicle.d0 where time < 104 and s0 < 99 and s2 < 16.0");
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1) + "," + resultSet.getString(d0s2);
                    // System.out.println(ans);
                    Assert.assertEquals(retArray[cnt], ans);
                    cnt++;
                }
                //Assert.assertEquals(cnt, 8);
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

