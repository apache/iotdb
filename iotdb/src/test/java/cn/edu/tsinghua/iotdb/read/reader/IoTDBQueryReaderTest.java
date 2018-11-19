package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.read.QueryEngine;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBQueryReaderTest {

    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d0s4 = "root.vehicle.d0.s4";
    private final String d0s5 = "root.vehicle.d0.s5";

    private final String d1s0 = "root.vehicle.d1.s0";
    private final String d1s1 = "root.vehicle.d1.s1";

    private static String[] stringValue = new String[]{"A", "B", "C", "D", "E"};
    private static String[] booleanValue = new String[]{"true", "false"};

    private static String[] create_sql = new String[]{
            "SET STORAGE GROUP TO root.vehicle",

            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s5 WITH DATATYPE=DOUBLE, ENCODING=RLE",

            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
    };

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;
    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    private int maxNumberOfPointsInPage;
    private int pageSizeInByte;
    private int groupSizeInByte;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();

            // use small page setting
            // origin value
            maxNumberOfPointsInPage = tsFileConfig.maxNumberOfPointsInPage;
            pageSizeInByte = tsFileConfig.pageSizeInByte;
            groupSizeInByte = tsFileConfig.groupSizeInByte;
            // new value
            tsFileConfig.maxNumberOfPointsInPage = 1000;
            tsFileConfig.pageSizeInByte = 1024 * 1024 * 150;
            tsFileConfig.groupSizeInByte = 1024 * 1024 * 1000;

            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(1000);
            //recovery value
            tsFileConfig.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
            tsFileConfig.pageSizeInByte = pageSizeInByte;
            tsFileConfig.groupSizeInByte = groupSizeInByte;
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException, IOException, FileNodeManagerException {

        if (testFlag) {
            Thread.sleep(1000);
            insertSQL();

            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            // select test
            selectAllTest();
            selectOneSeriesWithValueFilterTest();
            seriesTimeDigestReadTest();
            crossSeriesReadUpdateTest();

            connection.close();
        }
    }

    private void selectAllTest() throws IOException, FileNodeManagerException {
        String selectSql = "select * from root";
        System.out.println("selectAllTest===" + selectSql);

        QueryEngine queryEngine = new QueryEngine();
        QueryExpression queryExpression = QueryExpression.create();
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s0"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s1"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s2"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s3"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s4"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s5"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d1.s0"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d1.s1"));
        queryExpression.setQueryFilter(null);

        QueryDataSet queryDataSet = queryEngine.query(queryExpression);

        int cnt = 0;
        while (queryDataSet.hasNext()){
            RowRecord rowRecord = queryDataSet.next();
            String result = rowRecord.toString();
//            System.out.println("===" + result);
            cnt++;
        }
        assertEquals(23400, cnt);
    }

    private void selectOneSeriesWithValueFilterTest() throws ClassNotFoundException, SQLException, IOException, FileNodeManagerException {

        String selectSql = "select s0 from root.vehicle.d0 where s0 >= 20";
        System.out.println("selectOneSeriesWithValueFilterTest===" + selectSql);

        QueryEngine queryEngine = new QueryEngine();
        QueryExpression queryExpression = QueryExpression.create();
        Path p = new Path("root.vehicle.d0.s0");
        queryExpression.addSelectedPath(p);
        SeriesFilter seriesFilter = new SeriesFilter(p, ValueFilter.gtEq(20));
        queryExpression.setQueryFilter(seriesFilter);

        QueryDataSet queryDataSet = queryEngine.query(queryExpression);

        int cnt = 0;
        while (queryDataSet.hasNext()){
            RowRecord rowRecord = queryDataSet.next();
            String result = rowRecord.toString();
            System.out.println("selectOneSeriesWithValueFilterTest==="+ cnt + result);
            cnt++;
        }
        assertEquals(16440, cnt);

    }

    // https://github.com/thulab/iotdb/issues/192
    private void seriesTimeDigestReadTest() throws IOException, FileNodeManagerException {
        String selectSql = "select s0 from root.vehicle.d0 where time >= 22987";
        System.out.println("seriesTimeDigestReadTest===" + selectSql);

        // [3000, 13599] , [13700,23999]

        QueryEngine queryEngine = new QueryEngine();
        QueryExpression queryExpression = QueryExpression.create();
        Path p = new Path("root.vehicle.d0.s0");
        queryExpression.addSelectedPath(p);
        SeriesFilter seriesFilter = new SeriesFilter(p, TimeFilter.gt((long)22987));
        queryExpression.setQueryFilter(seriesFilter);

        QueryDataSet queryDataSet = queryEngine.query(queryExpression);

        int cnt = 0;
        while (queryDataSet.hasNext()){
            RowRecord rowRecord = queryDataSet.next();
            String result = rowRecord.toString();
            System.out.println("===" + result);
            cnt++;
        }
        assertEquals(3012, cnt);

    }

    private void crossSeriesReadUpdateTest() throws IOException, FileNodeManagerException {
        System.out.println("select s1 from root.vehicle.d0 where s0 < 111");
        QueryEngine queryEngine = new QueryEngine();
        QueryExpression queryExpression = QueryExpression.create();
        Path p0 = new Path("root.vehicle.d0.s0");
        Path p1 = new Path("root.vehicle.d0.s1");
        queryExpression.addSelectedPath(p1);
        SeriesFilter seriesFilter = new SeriesFilter(p0, ValueFilter.lt(111));
        queryExpression.setQueryFilter(seriesFilter);

        QueryDataSet queryDataSet = queryEngine.query(queryExpression);

        int cnt = 0;
        while (queryDataSet.hasNext()){
            RowRecord rowRecord = queryDataSet.next();
            System.out.println("~~~~" + rowRecord);
            long time = rowRecord.getTimestamp();
            String value = rowRecord.getFields().get(p1).getStringValue();
            if (time > 23000 && time < 100100) {
                System.out.println("~~~~" + time + "," + value);
                assertEquals("11111111", value);
            }
            //System.out.println("===" + rowRecord.toString());
            cnt++;
        }
        assertEquals(22800, cnt);
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
                //System.out.println("===" + time);
                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time, booleanValue[time % 2]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
                statement.execute(sql);
            }

            // statement.execute("flush");

            // insert large amount of data time range : 13700 ~ 24000
            for (int time = 13700; time < 24000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
                statement.execute(sql);
            }

            statement.execute("merge");

            Thread.sleep(5000);

            // buffwrite data, unsealed file
            for (int time = 100000; time < 101000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
                statement.execute(sql);
            }

            statement.execute("flush");

            // bufferwrite data, memory data
            for (int time = 200000; time < 201000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
                statement.execute(sql);
            }

            // overflow insert, time < 3000
            for (int time = 2000; time < 2500; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
                statement.execute(sql);
            }

            // overflow insert, time > 200000
            for (int time = 200900; time < 201000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 7777);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 8888);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, "goodman");
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time, booleanValue[time % 2]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, 9999);
                statement.execute(sql);
            }

            // overflow update
            statement.execute("UPDATE root.vehicle SET d0.s1 = 11111111 WHERE time > 23000 and time < 100100");

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
