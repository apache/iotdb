package org.apache.iotdb.db.integration.aggregation;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Locale;

public class IoTDBAggregationVectorIT {
    private static String[] creationSqls =
            new String[] {
                    "SET STORAGE GROUP TO root.vehicle.d0",
                    "CREATE TIMESERIES root.vehicle.d0(" +
                            "s0 INT32 encoding=RLE compressor=SNAPPY, "+
                            "s1 INT64 encoding=RLE compressor=SNAPPY, "+
                            "s2 FLOAT encoding=RLE compressor=SNAPPY, "+
                            "s3 TEXT encoding=PLAIN compressor=SNAPPY, "+
                            "s4 BOOLEAN encoding=PLAIN compressor=SNAPPY)"
            };
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d0s4 = "root.vehicle.d0.s4";
    private String insertTemplate =
            "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)" + " VALUES(%d,%d,%d,%f,%s,%s)";
    private long prevPartitionInterval;

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.closeStatMonitor();
        prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
        IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(1000);
        EnvironmentUtils.envSetUp();
        Class.forName(Config.JDBC_DRIVER_NAME);
        prepareData();
    }

    @After
    public void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
        IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
    }



    private void prepareData() {
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {

            for (String sql : creationSqls) {
                statement.execute(sql);
            }
            // prepare BufferWrite file
            for (int i = 5000; i < 7000; i++) {
                statement.execute(
                        String.format(
                                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
            }
            statement.execute("FLUSH");
            for (int i = 7500; i < 8500; i++) {
                statement.execute(
                        String.format(
                                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
            }
            statement.execute("FLUSH");
            // prepare Unseq-File
            for (int i = 500; i < 1500; i++) {
                statement.execute(
                        String.format(
                                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
            }
            statement.execute("FLUSH");
            for (int i = 3000; i < 6500; i++) {
                statement.execute(
                        String.format(
                                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
            }
            statement.execute("merge");
            // prepare BufferWrite cache
            for (int i = 9000; i < 10000; i++) {
                statement.execute(
                        String.format(
                                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
            }
            // prepare Overflow cache
            for (int i = 2000; i < 2500; i++) {
                statement.execute(
                        String.format(
                                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
