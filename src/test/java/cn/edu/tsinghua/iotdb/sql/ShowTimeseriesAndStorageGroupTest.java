package cn.edu.tsinghua.iotdb.sql;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/*
  test 'SHOW TIMESERIES <PATH>'
 */
public class ShowTimeseriesAndStorageGroupTest {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            /*
            preparations:
            SET STORAGE GROUP TO root.ln.wf01.wt01
            CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN
            CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH
            DATATYPE = FLOAT, ENCODING = RLE, COMPRESSOR = SNAPPY, MAX_POINT_NUMBER = 3
            INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
            INSERT INTO root.ln.wf01.wt01(timestamp, status) VALUES(NOW(), false)
            INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES(2017 - 11 - 01T00:17:00.000 + 08:00, 24.22028)
            INSERT INTO root.ln.wf01.wt01(timestamp, status, temperature) VALUES(1509466680000, false, 20.060787);
            */
            String[] sqls = new String[]{"show timeseries root.ln.wf01.wt01.status",// full path
                    "show timeseries root.ln", // prefix path
                    "show timeseries root.ln.*.wt01", // path with stars
                    "show timeseries root.a.b", // nonexistent timeseries
                    "show timeseries root.ln,root.ln", // SHOW TIMESERIES <PATH> only accept single path
                    "show timeseries  root.ln  root.ln.wf01", // SHOW TIMESERIES <PATH> only accept single path
                    "show timeseries", // not supported in jdbc
                    "show storage group"};
            for (String sql : sqls) {
                System.out.println("sql:" + sql);
                try {
                    boolean hasResultSet = statement.execute(sql);
                    if (hasResultSet) {
                        ResultSet resultSet = statement.getResultSet();
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        while (resultSet.next()) {
                            StringBuilder builder = new StringBuilder();
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                builder.append(resultSet.getString(i)).append(",");
                            }
                            System.out.println(builder);
                        }
                    }
                    System.out.println("");
                } catch (SQLException e) {
                    System.out.println(e);
                    System.out.println("");
                }
            }

            statement.close();

        } finally {
            connection.close();
        }
    }

}