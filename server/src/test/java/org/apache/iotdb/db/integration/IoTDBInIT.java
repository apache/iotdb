package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class IoTDBInIT {

    private static String[] sqls =
            new String[] {
                    "set storage group to root.ln",
                    "create timeseries root.ln.a.sg1.qrcode with datatype=TEXT,encoding=PLAIN",
                    "insert into root.ln.a.sg1(timestamp,qrcode) values(1509465600000,\"qrcode001\")",
                    "insert into root.ln.a.sg1(timestamp,qrcode) values(1509465660000,\"qrcode002\")",
                    "insert into root.ln.a.sg1(timestamp,qrcode) values(1509465720000,\"qrcode003\")",
                    "insert into root.ln.a.sg1(timestamp,qrcode) values(1509465780000,\"qrcode004\")",
                    "create timeseries root.ln.a.sg2.qrcode with datatype=TEXT,encoding=PLAIN",
                    "insert into root.ln.a.sg2(timestamp,qrcode) values(1509465720000,\"qrcode002\")",
                    "insert into root.ln.a.sg2(timestamp,qrcode) values(1509465780000,\"qrcode003\")",
                    "insert into root.ln.a.sg2(timestamp,qrcode) values(1509465840000,\"qrcode004\")",
                    "insert into root.ln.a.sg2(timestamp,qrcode) values(1509465900000,\"qrcode005\")",
                    "create timeseries root.ln.b.sg1.qrcode with datatype=TEXT,encoding=PLAIN",
                    "insert into root.ln.b.sg1(timestamp,qrcode) values(1509465780000,\"qrcode002\")",
                    "insert into root.ln.b.sg1(timestamp,qrcode) values(1509465840000,\"qrcode003\")",
                    "insert into root.ln.b.sg1(timestamp,qrcode) values(1509465900000,\"qrcode004\")",
                    "insert into root.ln.b.sg1(timestamp,qrcode) values(1509465960000,\"qrcode005\")"
            };

    @BeforeClass
    public static void setUp() throws Exception {
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.envSetUp();

        importData();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
    }

    private static void importData() throws ClassNotFoundException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {

            for (String sql : sqls) {
                statement.execute(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void selectWithStarTest1() throws ClassNotFoundException {
        String[] retArray =
                new String[] {
                        "1509465720000,qrcode003,qrcode002,"
                };

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute("select qrcode from root.ln.a.* where qrcode in ('qrcode002', 'qrcode003')");
            Assert.assertTrue(hasResultSet);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData,
                                "Time,root.ln.a.sg1.qrcode,root.ln.a.sg2.qrcode,",
                                new int[] {
                                        Types.TIMESTAMP,
                                        Types.VARCHAR,
                                        Types.VARCHAR,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void selectWithStarTest2() throws ClassNotFoundException {
        String[] retArray =
                new String[] {
                        "1509465780000,qrcode004,qrcode003,qrcode002,"
                };

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute("select qrcode from root.ln.*.* where qrcode in ('qrcode002', 'qrcode003', 'qrcode004')");
            Assert.assertTrue(hasResultSet);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData,
                                "Time,root.ln.a.sg1.qrcode,root.ln.a.sg2.qrcode,root.ln.b.sg1.qrcode,",
                                new int[] {
                                        Types.TIMESTAMP,
                                        Types.VARCHAR,
                                        Types.VARCHAR,
                                        Types.VARCHAR,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void selectWithAlignByDeviceTest() throws ClassNotFoundException {
        String[] retArray =
                new String[] {
                        "1509465660000,root.ln.a.sg1,qrcode002,",
                        "1509465780000,root.ln.a.sg1,qrcode004,",
                        "1509465720000,root.ln.a.sg2,qrcode002,",
                        "1509465840000,root.ln.a.sg2,qrcode004,",
                        "1509465780000,root.ln.b.sg1,qrcode002,",
                        "1509465900000,root.ln.b.sg1,qrcode004,",
                };

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute("select qrcode from root.ln.*.* where qrcode in ('qrcode002', 'qrcode004') align by device");
            Assert.assertTrue(hasResultSet);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData,
                                "Time,Device,qrcode,",
                                new int[] {
                                        Types.TIMESTAMP,
                                        Types.VARCHAR,
                                        Types.VARCHAR,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(6, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private List<Integer> checkHeader(
            ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
            throws SQLException {
        String[] expectedHeaders = expectedHeaderStrings.split(",");
        Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
        for (int i = 0; i < expectedHeaders.length; ++i) {
            expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
        }

        List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
            Assert.assertNotNull(typeIndex);
            Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
            actualIndexToExpectedIndexList.add(typeIndex);
        }
        return actualIndexToExpectedIndexList;
    }

}
