package cn.edu.tsinghua.iotdb.service;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.jdbc.TsfileConnection;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class IoTDBTimeZoneTest {
    private IoTDB deamon;
    private final String TIMESTAMP_STR = "Time";
    private final String tz1 = "root.timezone.tz1";

    private boolean testFlag = TestUtils.testFlag;

    private static String[] insertSqls = new String[]{
            "SET STORAGE GROUP TO root.timezone",
            "CREATE TIMESERIES root.timezone.tz1 WITH DATATYPE = INT32, ENCODING = PLAIN",
    };

    String[] retArray = new String[]{
    		"1514775603000,4",
    		"1514779200000,1",
    		"1514779201000,2",
    		"1514779202000,3",
    		"1514779203000,8",
    		"1514782804000,5",
    		"1514782805000,7",
    		"1514782806000,9",
    		"1514782807000,10",
    		"1514782808000,11",
    		"1514782809000,12",
    		"1514782810000,13",
    		"1514789200000,6",
    };
    
    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
            createTimeseries();
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
    /**
     *  // execute in cli-tool
	    SET STORAGE GROUP TO root.timezone
	    CREATE TIMESERIES root.timezone.tz1 WITH DATATYPE = INT32, ENCODING = PLAIN
	    set time_zone=+08:00
	    insert into root.timezone(timestamp,tz1) values(1514779200000,1)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T12:00:01,2)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T12:00:02+08:00,3)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T12:00:03+09:00,4)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T12:00:04+07:00,5)

	    set time_zone=+09:00
	    insert into root.timezone(timestamp,tz1) values(1514789200000,6)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T14:00:05,7)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T12:00:03+08:00,8)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T12:00:04+07:00,9)
	    set time_zone=Asia/Almaty
	    insert into root.timezone(timestamp,tz1) values(1514782807000,10)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T11:00:08,11)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T13:00:09+08:00,12)
	    insert into root.timezone(timestamp,tz1) values(2018-1-1T12:00:10+07:00,13)

	    select * from root
     */
    @Test
    public void timezoneTest() throws ClassNotFoundException, SQLException, TException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        TsfileConnection connection = null;
        try {
            connection = (TsfileConnection) DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String insertSQLTemplate = "insert into root.timezone(timestamp,tz1) values(%s,%s)";
            connection.setTimeZone("+08:00");
            // 1514779200000 = 2018-1-1T12:00:00+08:00
            statement.execute(String.format(insertSQLTemplate, "1514779200000", "1"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:01", "2"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:02+08:00", "3"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:03+09:00", "4"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:04+07:00", "5"));
            
            connection.setTimeZone("+09:00");
            statement.execute(String.format(insertSQLTemplate, "1514789200000", "6"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T14:00:05", "7"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:03+08:00", "8"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:06+07:00", "9"));
            
            // Asia/Almaty +06:00
            connection.setTimeZone("Asia/Almaty");
            statement.execute(String.format(insertSQLTemplate, "1514782807000", "10"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T11:00:08", "11"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T13:00:09+08:00", "12"));
            statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:10+07:00", "13"));
            
            boolean hasResultSet = statement.execute("select * from root");
            Assert.assertTrue(hasResultSet);

            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(tz1);
                Assert.assertEquals(retArray[cnt], ans);
                cnt++;
            }
            Assert.assertEquals(13, cnt);
            statement.close();
        } finally {
            connection.close();
        }
    }
    
    public void createTimeseries() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : insertSqls) {
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
}
