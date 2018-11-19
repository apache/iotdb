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
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class IoTDBSequenceDataQueryTest {
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
    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    private int maxNumberOfPointsInPage;
    private int pageSizeInByte;
    private int groupSizeInByte;

    private int d0s0gteq14 = 0;

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
            tsFileConfig.maxNumberOfPointsInPage = 100;
            tsFileConfig.pageSizeInByte = 1024 * 1024 * 150;
            tsFileConfig.groupSizeInByte = 1024 * 1024 * 100;

            daemon = IoTDB.getInstance();
            daemon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            daemon.stop();
            Thread.sleep(1000);

            //recovery value
            tsFileConfig.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
            tsFileConfig.pageSizeInByte = pageSizeInByte;
            tsFileConfig.groupSizeInByte = groupSizeInByte;

            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void test() throws InterruptedException, SQLException, ClassNotFoundException, IOException, FileNodeManagerException {
        if (testFlag) {
            Thread.sleep(5000);
            insertData();
            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            TsFilesReaderWithoutFilterTest();
            TsFilesReaderWithTimeFilterTest();
            TsFilesReaderWithValueFilterTest();


            connection.close();
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

            //insert data (time from 300-999)
            for(long time = 300; time < 1000; time++){
                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[(int)time % 5]);
                statement.execute(sql);

                if(time % 17 >= 14){
                    d0s0gteq14++;
                }
            }

            statement.execute("flush");

            //insert data (time from 1200-1499)
            for(long time = 1200; time < 1500; time++){
                String sql = null;
                if(time % 2 == 0){
                    sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
                    statement.execute(sql);
                    sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
                    statement.execute(sql);
                    if(time % 17 >= 14){
                        d0s0gteq14++;
                    }
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


    private void TsFilesReaderWithoutFilterTest() throws IOException, FileNodeManagerException {
        String sql = "select * from root";
        QueryEngine queryEngine = new QueryEngine();
        QueryExpression queryExpression = QueryExpression.create();
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s0"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s1"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s2"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s3"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s4"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d1.s0"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d1.s1"));
        queryExpression.setQueryFilter(null);

        QueryDataSet queryDataSet = queryEngine.query(queryExpression);

        int cnt = 0;
        while (queryDataSet.hasNext()){
            RowRecord rowRecord = queryDataSet.next();
            System.out.println("===" + rowRecord.toString());
            cnt++;
        }
        assertEquals(1000, cnt);

    }

    private void TsFilesReaderWithValueFilterTest() throws IOException, FileNodeManagerException {
        String sql = "select * from root where root.vehicle.d0.s0 >=14";
        QueryEngine queryEngine = new QueryEngine();
        QueryExpression queryExpression = QueryExpression.create();
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s0"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s1"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s2"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s3"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d0.s4"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d1.s0"));
        queryExpression.addSelectedPath(new Path("root.vehicle.d1.s1"));

        Path p = new Path("root.vehicle.d0.s0");
        SeriesFilter seriesFilter = new SeriesFilter(p, ValueFilter.gtEq(14));
        queryExpression.setQueryFilter(seriesFilter);

        QueryDataSet queryDataSet = queryEngine.query(queryExpression);

        int cnt = 0;
        while (queryDataSet.hasNext()){
            RowRecord rowRecord = queryDataSet.next();
            System.out.println("TsFilesReaderWithValueFilterTest===" + rowRecord.toString());
            cnt++;
        }
        assertEquals(d0s0gteq14, cnt);
    }

    private void TsFilesReaderWithTimeFilterTest() throws IOException, FileNodeManagerException {
        QueryEngine queryEngine = new QueryEngine();
        QueryExpression queryExpression = QueryExpression.create();
        Path d0s0 = new Path("root.vehicle.d0.s0");
        Path d1s0 = new Path("root.vehicle.d1.s0");
        Path d1s1 = new Path("root.vehicle.d1.s1");
        queryExpression.addSelectedPath(d0s0);
        queryExpression.addSelectedPath(d1s0);
        queryExpression.addSelectedPath(d1s1);

        GlobalTimeFilter globalTimeFilter = new GlobalTimeFilter(TimeFilter.gtEq((long) 800));
        queryExpression.setQueryFilter(globalTimeFilter);
        QueryDataSet queryDataSet = queryEngine.query(queryExpression);

        int cnt = 0;
        while (queryDataSet.hasNext()){
            RowRecord rowRecord = queryDataSet.next();
            String strd0s0 = ""+rowRecord.getFields().get(d0s0).getValue();
            long time = rowRecord.getTimestamp();
            System.out.println(time+"===" + rowRecord.toString());
            assertEquals(""+time % 17,strd0s0);
            cnt++;
        }
        assertEquals(350, cnt);
    }

}
