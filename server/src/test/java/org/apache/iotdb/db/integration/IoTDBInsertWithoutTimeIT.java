package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** @Author: Architect @Date: 2021-07-13 16:32 */
public class IoTDBInsertWithoutTimeIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    initCreateSQLStatement();
    EnvironmentUtils.envSetUp();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvironmentUtils.cleanEnv();
  }

  private static void close() {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void initCreateSQLStatement() {
    sqls.add("SET STORAGE GROUP TO root.t1");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
  }

  private static void insertData() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }

    statement.close();
  }

  @Test
  public void testInsertWithoutTime() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute("insert into root.t1.wf01.wt01(time, status, temperature) values (1, true, 22)");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, status, temperature) values (2, false, 33),(3, true, 44)");
    st0.execute("insert into root.t1.wf01.wt01(status, temperature) values (true, 10)");
    st0.execute("insert into root.t1.wf01.wt01(status) values (false)");

    Statement st1 = connection.createStatement();
    ResultSet rs1 = st1.executeQuery("select count(status) from root.t1.wf01.wt01");
    rs1.next();
    long countStatus = rs1.getLong(1);
    Assert.assertEquals(countStatus, 5L);

    st1.close();
  }

  @Test(expected = Exception.class)
  public void testInsertWithTimesColumns() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(time) values (1)");
  }

  @Test(expected = Exception.class)
  public void testInsertMultiRow() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(status) values (false),(true)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithMultiTimesColumns1() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(time,time) values(1,1)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithMultiTimesColumns2() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(time,status,time) values(1,false,1)");
  }
}
