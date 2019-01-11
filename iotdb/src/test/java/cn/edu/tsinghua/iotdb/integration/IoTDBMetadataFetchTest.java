package cn.edu.tsinghua.iotdb.integration;

import cn.edu.tsinghua.iotdb.jdbc.Constant;
import cn.edu.tsinghua.iotdb.jdbc.Config;
import cn.edu.tsinghua.iotdb.jdbc.IoTDBMetadataResultSet;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test.
 * All test which will start the IoTDB server should be defined as integration test.
 */
public class IoTDBMetadataFetchTest {

    private static IoTDB deamon;

    private DatabaseMetaData databaseMetaData;

    private static void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            String[] insertSqls = new String[]{
                    "SET STORAGE GROUP TO root.ln.wf01.wt01",
                    "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
                    "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, " +
                            "COMPRESSOR = SNAPPY, MAX_POINT_NUMBER = 3"
            };

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

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.closeMemControl();

        deamon = IoTDB.getInstance();
        deamon.active();
        EnvironmentUtils.envSetUp();

        insertSQL();
    }

    @After
    public void tearDown() throws Exception {
        deamon.stop();
        Thread.sleep(5000);
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void ShowTimeseriesTest1() throws ClassNotFoundException, SQLException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String[] sqls = new String[]{
                    "show timeseries root.ln.wf01.wt01.status", // full seriesPath
                    "show timeseries root.ln", // prefix seriesPath
                    "show timeseries root.ln.*.wt01", // seriesPath with stars

                    "show timeseries root.a.b", // nonexistent timeseries, thus returning ""
                    "show timeseries root.ln,root.ln", // SHOW TIMESERIES <PATH> only accept single seriesPath, thus returning ""
            };
            String[] standards = new String[]{"root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n",

                    "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n" +
                            "root.ln.wf01.wt01.temperature,root.ln.wf01.wt01,FLOAT,RLE,\n",

                    "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n" +
                            "root.ln.wf01.wt01.temperature,root.ln.wf01.wt01,FLOAT,RLE,\n",

                    "",

                    ""
            };
            for (int n = 0; n < sqls.length; n++) {
                String sql = sqls[n];
                String standard = standards[n];
                StringBuilder builder = new StringBuilder();
                try {
                    boolean hasResultSet = statement.execute(sql);
                    if (hasResultSet) {
                        ResultSet resultSet = statement.getResultSet();
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        while (resultSet.next()) {
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                builder.append(resultSet.getString(i)).append(",");
                            }
                            builder.append("\n");
                        }
                    }
                    Assert.assertEquals(builder.toString(), standard);
                } catch (SQLException e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
            statement.close();
        } finally {
            connection.close();
        }
    }

    @Test
    public void ShowTimeseriesTest2() throws ClassNotFoundException, SQLException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            String sql = "show timeseries"; // not supported in jdbc, thus expecting SQLException
            statement.execute(sql);
        } catch (SQLException e) {
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            statement.close();
            connection.close();
        }
    }

    @Test
    public void ShowStorageGroupTest() throws ClassNotFoundException, SQLException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String[] sqls = new String[]{
                    "show storage group"
            };
            String[] standards = new String[]{
                    "root.ln.wf01.wt01,\n"
            };
            for (int n = 0; n < sqls.length; n++) {
                String sql = sqls[n];
                String standard = standards[n];
                StringBuilder builder = new StringBuilder();
                try {
                    boolean hasResultSet = statement.execute(sql);
                    if (hasResultSet) {
                        ResultSet resultSet = statement.getResultSet();
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        while (resultSet.next()) {
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                builder.append(resultSet.getString(i)).append(",");
                            }
                            builder.append("\n");
                        }
                    }
                    Assert.assertEquals(builder.toString(), standard);
                } catch (SQLException e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
            statement.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void DatabaseMetaDataTest() throws ClassNotFoundException, SQLException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX+"127.0.0.1:6667/", "root", "root");
            databaseMetaData = connection.getMetaData();

            AllColumns();
            Device();
            ShowTimeseriesPath1();
            ShowTimeseriesPath2();
            ShowStorageGroup();
            ShowTimeseriesInJson();

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
     * get all columns' name under a given seriesPath
     */
    private void AllColumns() throws SQLException {
        String standard = "Column,\n" +
                "root.ln.wf01.wt01.status,\n" +
                "root.ln.wf01.wt01.temperature,\n";

        ResultSet resultSet = databaseMetaData.getColumns(Constant.CatalogColumn, "root", null, null);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int colCount = resultSetMetaData.getColumnCount();
        StringBuilder resultStr = new StringBuilder();
        for (int i = 1; i < colCount + 1; i++) {
            resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        resultStr.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= colCount; i++) {
                resultStr.append(resultSet.getString(i)).append(",");
            }
            resultStr.append("\n");
        }
        Assert.assertEquals(resultStr.toString(), standard);
    }

    /**
     * get all delta objects under a given column
     */
    private void Device() throws SQLException {
        String standard = "Column,\n" +
                "root.ln.wf01.wt01,\n";

        ResultSet resultSet = databaseMetaData.getColumns(Constant.CatalogDevice, "ln", null, null);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int colCount = resultSetMetaData.getColumnCount();
        StringBuilder resultStr = new StringBuilder();
        for (int i = 1; i < colCount + 1; i++) {
            resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        resultStr.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= colCount; i++) {
                resultStr.append(resultSet.getString(i)).append(",");
            }
            resultStr.append("\n");
        }
        Assert.assertEquals(resultStr.toString(), standard);
    }

    /**
     * show timeseries <seriesPath>
     * usage 1
     */
    private void ShowTimeseriesPath1() throws SQLException {
        String standard = "Timeseries,Storage Group,DataType,Encoding,\n" +
                "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n" +
                "root.ln.wf01.wt01.temperature,root.ln.wf01.wt01,FLOAT,RLE,\n";

        ResultSet resultSet = databaseMetaData.getColumns(Constant.CatalogTimeseries, "root", null, null);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int colCount = resultSetMetaData.getColumnCount();
        StringBuilder resultStr = new StringBuilder();
        for (int i = 1; i < colCount + 1; i++) {
            resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        resultStr.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= colCount; i++) {
                resultStr.append(resultSet.getString(i)).append(",");
            }
            resultStr.append("\n");
        }
        Assert.assertEquals(resultStr.toString(), standard);
    }

    /**
     * show timeseries <seriesPath>
     * usage 2
     */
    private void ShowTimeseriesPath2() throws SQLException {
        String standard = "DataType,\n" +
                "BOOLEAN,\n";

        ResultSet resultSet = databaseMetaData.getColumns(Constant.CatalogTimeseries, "root.ln.wf01.wt01.status", null, null);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder resultStr = new StringBuilder();
        resultStr.append(resultSetMetaData.getColumnName(3)).append(",\n");
        while (resultSet.next()) {
            resultStr.append(resultSet.getString(IoTDBMetadataResultSet.GET_STRING_TIMESERIES_DATATYPE)).append(",");
            resultStr.append("\n");
        }
        Assert.assertEquals(resultStr.toString(), standard);
    }

    /**
     * show storage group
     */
    private void ShowStorageGroup() throws SQLException {
        String standard = "Storage Group,\n" +
                "root.ln.wf01.wt01,\n";

        ResultSet resultSet = databaseMetaData.getColumns(Constant.CatalogStorageGroup, null, null, null);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int colCount = resultSetMetaData.getColumnCount();
        StringBuilder resultStr = new StringBuilder();
        for (int i = 1; i < colCount + 1; i++) {
            resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        resultStr.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= colCount; i++) {
                resultStr.append(resultSet.getString(i)).append(",");
            }
            resultStr.append("\n");
        }
        Assert.assertEquals(resultStr.toString(), standard);
    }

    /**
     * show metadata in json
     */
    private void ShowTimeseriesInJson() {
        String metadataInJson = databaseMetaData.toString();
        String standard = "===  Timeseries Tree  ===\n" +
                "\n" +
                "root:{\n" +
                "    ln:{\n" +
                "        wf01:{\n" +
                "            wt01:{\n" +
                "                status:{\n" +
                "                     DataType: BOOLEAN,\n" +
                "                     Encoding: PLAIN,\n" +
                "                     args: {},\n" +
                "                     StorageGroup: root.ln.wf01.wt01 \n" +
                "                },\n" +
                "                temperature:{\n" +
                "                     DataType: FLOAT,\n" +
                "                     Encoding: RLE,\n" +
                "                     args: {COMPRESSOR=SNAPPY, MAX_POINT_NUMBER=3},\n" +
                "                     StorageGroup: root.ln.wf01.wt01 \n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Assert.assertEquals(metadataInJson, standard);
    }
}
