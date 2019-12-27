package org.apache.iotdb.db.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBSimpleQueryTest {
  private IoTDB deamon;

  @Before
  public void setUp() throws Exception {
    deamon = IoTDB.getInstance();
    deamon.active();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    deamon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testUnseqUnsealedDeleteQuery() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");

      // seq data
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1000, 1)");
      statement.execute("flush");

      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (%d, %d)", i, i));
      }

      statement.execute("flush");

      // unseq data
      for (int i = 11; i <= 20; i++) {
        statement.execute(
            String.format("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (%d, %d)", i, i));
      }

      statement.execute("delete from root.sg1.d0.s0 where time <= 15");

      ResultSet resultSet = statement.executeQuery("select * from root");

      long count = 0;

      while(resultSet.next()) {
        count++;
      }

      System.out.println(count);

    }
  }

}
