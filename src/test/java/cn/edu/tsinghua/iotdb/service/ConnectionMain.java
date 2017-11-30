package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

import java.sql.*;

import static cn.edu.tsinghua.iotdb.service.TestUtils.count;
import static cn.edu.tsinghua.iotdb.service.TestUtils.max_value;
import static org.junit.Assert.fail;

/**
 * test class
 * connection to remote server
 */
public class ConnectionMain {

    private static final String TIMESTAMP_STR = "Time";
    private static final String d_56 = "root.performf.group_5.d_56.s_94";
    private static final String d_66 = "root.performf.group_6.d_66.s_10";
    private static final String d_75 = "root.performf.group_7.d_75.s_13";
    private static final String err = "root.performf.group_6.d_65.s_71";

    static String sql1 = "SELECT max_value(s_94) FROM root.performf.group_5.d_56 GROUP BY(250000ms, 1262275200000,[1262275200000,1262276200000])";
    static String sql2 = "SELECT max_value(s_10) FROM root.performf.group_6.d_66 GROUP BY(250000ms, 1262275200000,[1262425699500,1262426699500])";
    static String sql3 = "SELECT max_value(s_13) FROM root.performf.group_7.d_75 WHERE root.performf.group_7.d_75.s_13 > 0.0  GROUP BY(250000ms, 1262275200000,[1262518199500,1262519199500])";

    static String error = "SELECT s_71 FROM root.performf.group_6.d_65 where s_71 > 0.0";
    String streamError =
            "SELECT max_value(s_71) FROM root.performf.group_6.d_65 WHERE root.performf.group_6.d_65.s_71 > 0.0  GROUP BY(250000ms, 1262275200000,[1262518199500,1262519199500])";
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.9:6667/", "root", "root");
            //connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement;
            boolean hasResultSet;

//            System.out.println("test 1 ======= ");
//            hasResultSet = statement.execute(sql1);
//            if (hasResultSet) {
//                ResultSet resultSet = statement.getResultSet();
//                int cnt = 1;
//                while (resultSet.next()) {
//                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d_56));
//                    System.out.println(ans);
//                    cnt ++;
//                }
//                System.out.println(cnt);
//            }
//            statement.close();
//
//            System.out.println("test 2 ======= ");
//            statement = connection.createStatement();
//            hasResultSet = statement.execute(sql2);
//            if (hasResultSet) {
//                ResultSet resultSet = statement.getResultSet();
//                int cnt = 1;
//                while (resultSet.next()) {
//                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d_66));
//                    System.out.println(ans);
//                    cnt ++;
//                }
//                System.out.println(cnt);
//            }
//            statement.close();

            System.out.println("test 3 ======= ");
            statement = connection.createStatement();
            hasResultSet = statement.execute(error);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(err);
                    System.out.println(ans);
                    cnt ++;
                }
                System.out.println(cnt);
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            if (connection != null) {
                connection.close();
            }
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}

