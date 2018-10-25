package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.read.timegenerator.TimeGenerator;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBTimeGeneratorTest {
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

    private int count = 0;
    private int count2 = 150;

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

            testOneSeriesWithValueAndTimeFilter();
            testEmptySeriesWithValueFilter();
            testMultiSeriesesWithValueFilterAndTimeFilter();

            connection.close();
        }
    }


    /**
     * value >= 14 && time > 500
     * */
    private void testOneSeriesWithValueAndTimeFilter() throws IOException, FileNodeManagerException {
        Path pd0s0 = new Path("root.vehicle.d0.s0");
        ValueFilter.ValueGtEq  valueGtEq = ValueFilter.gtEq(14);
        TimeFilter.TimeGt timeGt = TimeFilter.gt((long)500);

        QueryFilter queryFilter = new SeriesFilter(pd0s0, new cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.And( valueGtEq, timeGt));
        TimeGenerator timeGenerator = new TimeGenerator(queryFilter);
        System.out.println("root.vehicle.d0.s0 >= 14 && time > 500 ");
        int cnt = 0;
        while(timeGenerator.hasNext()){
            long time = timeGenerator.next();
            assertTrue(satisfyTimeFilter1(time));
            cnt++;
            System.out.println("cnt ="+cnt+"; time = "+time);
        }
        assertEquals(count, cnt);
    }

    /**
     * root.vehicle.d1.s0 >= 5
     * */
    public void testEmptySeriesWithValueFilter() throws IOException, FileNodeManagerException {
        Path pd1s0 = new Path("root.vehicle.d1.s0");
        ValueFilter.ValueGtEq  valueGtEq = ValueFilter.gtEq(5);

        QueryFilter queryFilter = new SeriesFilter(pd1s0, valueGtEq);
        TimeGenerator timeGenerator = new TimeGenerator(queryFilter);
        System.out.println("root.vehicle.d1.s0 >= 5");
        int cnt = 0;
        while(timeGenerator.hasNext()){
            long time = timeGenerator.next();
            assertTrue(satisfyTimeFilter1(time));
            cnt++;
            System.out.println("cnt ="+cnt+"; time = "+time);
        }
        assertEquals(0, cnt);
    }

    /**
     * root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11.5 || time > 900
     * */
    public void testMultiSeriesesWithValueFilterAndTimeFilter() throws IOException, FileNodeManagerException {
        Path pd0s0 = new Path("root.vehicle.d0.s0");
        Path pd0s2 = new Path("root.vehicle.d0.s2");

        ValueFilter.ValueGtEq  valueGtEq5 = ValueFilter.gtEq(5);
        ValueFilter.ValueGtEq  valueGtEq11 = ValueFilter.gtEq((float) 11.5);
        TimeFilter.TimeGt timeGt = TimeFilter.gt((long)900);

        SeriesFilter seriesFilterd0s0 = new SeriesFilter(pd0s0, new cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.Or( valueGtEq5, timeGt));
        SeriesFilter seriesFilterd0s2 = new SeriesFilter(pd0s2, new cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.Or( valueGtEq11, timeGt));

        QueryFilter queryFilter = QueryFilterFactory.and(seriesFilterd0s0, seriesFilterd0s2);
        TimeGenerator timeGenerator = new TimeGenerator(queryFilter);
        System.out.println("root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11.5 || time > 900");
        int cnt = 0;
        while(timeGenerator.hasNext()){
            long time = timeGenerator.next();
            assertTrue(satisfyTimeFilter2(time));
            cnt++;
            System.out.println("cnt ="+cnt+"; time = "+time);
        }
        assertEquals(count2, cnt);
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

                if(satisfyTimeFilter1(time)){
                    count++;
                }

                if(satisfyTimeFilter2(time)){
                    count2++;
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
                    if(satisfyTimeFilter1(time)){
                        count++;
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


    /**
     * value >= 14 && time > 500
     * */
    private boolean satisfyTimeFilter1(long time){
        return time % 17 >= 14 && time > 500;
    }

    /**
     * root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11 || time > 900
     * */
    private boolean satisfyTimeFilter2(long time){
        return (time % 17 >= 5 || time > 900) && (time % 31 >= 11.5 || time > 900);
    }
}
