package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.jdbc.TsfileMetadataResultSet;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class MetadataFetchTest {
    private IoTDB deamon;

    private DatabaseMetaData databaseMetaData;

    private boolean testFlag = TestUtils.testFlag;

    private static String[] insertSqls = new String[]{
            "SET STORAGE GROUP TO root.ln.wf01.wt01",
            "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, COMPRESSOR = SNAPPY, MAX_POINT_NUMBER = 3"
    };

    public void insertSQL() throws ClassNotFoundException, SQLException {
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

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
            insertSQL();
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

    @Test
    public void ShowTimeseriesTest1() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String[] sqls = new String[]{
                    "show timeseries root.ln.wf01.wt01.status", // full path
                    "show timeseries root.ln", // prefix path
                    "show timeseries root.ln.*.wt01", // path with stars

                    "show timeseries root.a.b", // nonexistent timeseries, thus returning ""
                    "show timeseries root.ln,root.ln", // SHOW TIMESERIES <PATH> only accept single path, thus returning ""
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

    @Test(expected = SQLException.class)
    public void ShowTimeseriesTest2() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();
        String sql = "show timeseries"; // not supported in jdbc, thus expecting SQLException
        boolean hasResultSet = statement.execute(sql);
        statement.close();
        connection.close();
    }

    @Test
    public void ShowStorageGroupTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
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
            connection.close();
        }
    }

    @Test
    public void DatabaseMetaDataTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            databaseMetaData = connection.getMetaData();

            AllColumns();
            DeltaObject();
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
     * get all columns' name under a given path
     */
    public void AllColumns() throws SQLException {
        String standard = "Column,\n" +
                "root.ln.wf01.wt01.status,\n" +
                "root.ln.wf01.wt01.temperature,\n";

        ResultSet resultSet = databaseMetaData.getColumns(TsFileDBConstant.CatalogColumn, "root", null, null);
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
    public void DeltaObject() throws SQLException {
        String standard = "Column,\n" +
                "root.ln.wf01.wt01,\n";

        ResultSet resultSet = databaseMetaData.getColumns(TsFileDBConstant.CatalogDeltaObject, "ln", null, null);
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
     * show timeseries <path>
     * usage 1
     */
    public void ShowTimeseriesPath1() throws SQLException {
        String standard = "Timeseries,Storage Group,DataType,Encoding,\n" +
                "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n" +
                "root.ln.wf01.wt01.temperature,root.ln.wf01.wt01,FLOAT,RLE,\n";

        ResultSet resultSet = databaseMetaData.getColumns(TsFileDBConstant.CatalogTimeseries, "root", null, null);
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
     * show timeseries <path>
     * usage 2
     */
    public void ShowTimeseriesPath2() throws SQLException {
        String standard = "DataType,\n" +
                "BOOLEAN,\n";

        ResultSet resultSet = databaseMetaData.getColumns(TsFileDBConstant.CatalogTimeseries, "root.ln.wf01.wt01.status", null, null);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder resultStr = new StringBuilder();
        resultStr.append(resultSetMetaData.getColumnName(3)).append(",\n");
        while (resultSet.next()) {
            resultStr.append(resultSet.getString(TsfileMetadataResultSet.GET_STRING_TIMESERIES_DATATYPE)).append(",");
            resultStr.append("\n");
        }
        Assert.assertEquals(resultStr.toString(), standard);
    }

    /**
     * show storage group
     */
    public void ShowStorageGroup() throws SQLException {
        String standard = "Storage Group,\n" +
                "root.ln.wf01.wt01,\n";

        ResultSet resultSet = databaseMetaData.getColumns(TsFileDBConstant.CatalogStorageGroup, null, null, null);
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
    public void ShowTimeseriesInJson() {
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
