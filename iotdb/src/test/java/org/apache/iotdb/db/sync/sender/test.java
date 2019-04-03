package org.apache.iotdb.db.sync.sender;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.integration.Constant;
import org.apache.iotdb.jdbc.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The test is to run a complete sync function Before you run the test, make sure receiver has been
 * cleaned up and inited.
 */
public class test {

  private static final String[] sqls1 = new String[]{
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d2.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d2.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d2.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d2.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d3.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d3.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d3.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d3.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d4.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d4.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d4.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d5.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d5.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d5.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d5.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d1.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d2.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d3.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d4.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d5.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN"};
  private static final String[] sqls2 = new String[]{
      "CREATE TIMESERIES root.vehicle.d6.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d7.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d8.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d9.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d10.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d11.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d11.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d6.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d6.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d7.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d7.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d8.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d9.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN"
  };
  private static final String[] sqls3 = new String[]{
      "CREATE TIMESERIES root.iotdb.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d2.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d2.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d3.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d3.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d4.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d4.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d5.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d5.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d6.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d6.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d7.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d7.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.flush.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.flush.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.flush.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.flush.d1.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.flush.d2.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.flush.d2.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
  };
  private static boolean testFlag = Constant.testFlag;
  private static String serverIpTest = "192.168.130.14";
  private static boolean success = false;
  private static final Logger logger = LoggerFactory.getLogger(SingleClientSyncTest.class);

  public static void main(String[] args) throws Exception {
    test();
    System.exit(0);
  }

  public static void test() throws ClassNotFoundException, SQLException {
    Set<String> local = new HashSet<>();
    Set<String> cluster = new HashSet<>();
    if (testFlag) {
      // the first time to sync
      logger.debug("It's the first time to sync!");
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                "root")) {
          Statement statement = connection.createStatement();
          statement.execute("SET STORAGE GROUP TO root.vehicle");
          statement.execute("SET STORAGE GROUP TO root.test");
          statement.execute("SET STORAGE GROUP TO root.iotdb");
          statement.execute("SET STORAGE GROUP TO root.flush");
          for (String sql : sqls1) {
            statement.addBatch(sql);
          }
          statement.executeBatch();
          statement.close();
        }
      } catch (SQLException | ClassNotFoundException e) {
        fail(e.getMessage());
      }

      try {
        try (Connection connection = DriverManager
            .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root",
                "root")) {
          Statement statement = connection.createStatement();
          statement.execute("SET STORAGE GROUP TO root.vehicle");
          statement.execute("SET STORAGE GROUP TO root.test");
          statement.execute("SET STORAGE GROUP TO root.iotdb");
          statement.execute("SET STORAGE GROUP TO root.flush");
          for (String sql : sqls1) {
            statement.addBatch(sql);
          }
          statement.executeBatch();
          statement.close();
        }
      } catch (SQLException e) {
        fail(e.getMessage());
      }

      // Compare data of sender and receiver
      local.clear();
      try (Connection connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
              "root")) {
        Statement statement = connection.createStatement();
        boolean hasResultSet = statement.execute("show timeseries root");
        if (hasResultSet) {
          ResultSet res = statement.getResultSet();
          while (res.next()) {
            local.add(res.getString("Timeseries") + res.getString("Storage Group")
                + res.getString("DataType") + res.getString("Encoding"));
          }
        }
        statement.close();
      } catch (Exception e) {
        logger.error("", e);
      }

      cluster.clear();
      try {
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root",
                  "root");
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("show timeseries root");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              cluster.add(res.getString("Timeseries") + res.getString("Storage Group")
                  + res.getString("DataType") + res.getString("Encoding"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      } catch (SQLException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }
      logger.debug(String.valueOf(local.size()));
      logger.debug(String.valueOf(cluster.size()));
      logger.debug(local.toString());
      logger.debug(cluster.toString());
      if (!(local.size() == cluster.size() && local.containsAll(cluster))) {
        success = false;
        return;
      }

      // the second time to sync
      logger.debug("It's the second time to sync!");
      try {
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                  "root");
          Statement statement = connection.createStatement();
          for (String sql : sqls2) {
            statement.addBatch(sql);
          }
          statement.executeBatch();
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      } catch (SQLException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }

      try {
        try (Connection connection = DriverManager
            .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root",
                "root")) {
          Statement statement = connection.createStatement();
          for (String sql : sqls2) {
            statement.addBatch(sql);
          }
          statement.executeBatch();
          statement.close();
        }
      } catch (SQLException e) {
        fail(e.getMessage());
      }
//      fileSenderImpl.sync();

      // Compare data of sender and receiver
      local.clear();
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                  "root");
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("show timeseries root");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              local.add(res.getString("Timeseries") + res.getString("Storage Group")
                  + res.getString("DataType") + res.getString("Encoding"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      } catch (ClassNotFoundException | SQLException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }

      cluster.clear();
      {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root",
                  "root");
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("show timeseries root");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              cluster.add(res.getString("Timeseries") + res.getString("Storage Group")
                  + res.getString("DataType") + res.getString("Encoding"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      }
      logger.debug(String.valueOf(local.size()));
      logger.debug(String.valueOf(cluster.size()));
      logger.debug(local.toString());
      logger.debug(cluster.toString());
      if (!(local.size() == cluster.size() && local.containsAll(cluster))) {
        success = false;
        return;
      }

      // the third time to sync
      logger.debug("It's the third time to sync!");
      try (Connection connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
              "root")) {
        Statement statement = connection.createStatement();
        for (String sql : sqls3) {
          statement.addBatch(sql);
        }
        statement.executeBatch();
        statement.close();
      } catch (Exception e) {
        logger.error("", e);
      }

      try {
        try (Connection connection = DriverManager
            .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root",
                "root")) {
          Statement statement = connection.createStatement();
          for (String sql : sqls3) {
            statement.addBatch(sql);
          }
          statement.executeBatch();
          statement.close();
        }
      } catch (SQLException e) {
        fail(e.getMessage());
      }

      // Compare data of sender and receiver
      local.clear();
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection = DriverManager
            .getConnection(String.format("jdbc:iotdb://%s:6667/", "127.0.0.1"), "root",
                "root")) {
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("show timeseries root");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              local.add(res.getString("Timeseries") + res.getString("Storage Group")
                  + res.getString("DataType") + res.getString("Encoding"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        }
      } catch (ClassNotFoundException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }

      cluster.clear();
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root", "root");
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("show timeseries root");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              cluster.add(res.getString("Timeseries") + res.getString("Storage Group")
                  + res.getString("DataType") + res.getString("Encoding"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      } catch (ClassNotFoundException | SQLException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }
      logger.debug(String.valueOf(local.size()));
      logger.debug(String.valueOf(cluster.size()));
      logger.debug(String.valueOf(local));
      logger.debug(String.valueOf(cluster));
      if (!(local.size() == cluster.size() && local.containsAll(cluster))) {
        success = false;
      }
    }
  }
}
