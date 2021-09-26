package org.apache.iotdb.db.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBSettleIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    initCreateSQLStatement();
    EnvironmentUtils.envSetUp();
    executeSql();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void onlineSettleTest(){
    PartialPath sg= null;
    try {
      sg = new PartialPath("root.st1");
      StorageEngine.getInstance().settleAll(sg);
    } catch (IllegalPathException | StorageEngineException | WriteProcessException e) {
      Assert.fail(e.getMessage());
    }
  }

  private static void close() throws SQLException {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void initCreateSQLStatement() {
    sqls.add("SET STORAGE GROUP TO root.st1");
    sqls.add("CREATE TIMESERIES root.st1.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    for(int i=1;i<=10;i++){
      sqls.add("insert into root.st1.wf01.wt01(timestamp,status) values("+100*i+",false)");
    }
    sqls.add("flush");
    sqls.add("delete from root.st1.wf01.wt01.* where time<500");
  }

  private static void executeSql() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }

    statement.close();
  }


}
