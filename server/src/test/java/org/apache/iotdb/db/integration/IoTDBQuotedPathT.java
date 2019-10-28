package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;

import static org.apache.iotdb.db.integration.Constant.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IoTDBQuotedPathT {
  private IoTDB daemon;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                    "root");
         Statement statement = connection.createStatement()) {

      String[] exp = new String[]{
              "1509465600000,true",
              "1509465600001,true",
              "1509465600002,false",
              "1509465600003,false"
      };
      statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt01");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.\"status.2.3\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.ln.wf01.wt01(timestamp,\"status.2.3\") values(1509465600000,true)");
      statement.execute("INSERT INTO root.ln.wf01.wt01(timestamp,\'status.2.3\') values(1509465600001,true)");
      statement.execute("INSERT INTO root.ln.wf01.wt01(timestamp,\"status.2.3\") values(1509465600002,false)");
      statement.execute("INSERT INTO root.ln.wf01.wt01(timestamp,\'status.2.3\') values(1509465600003,false)");
      statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt02");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt02.\"abd\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt02.\"asf.asd.sdf\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt02.\"asd12\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      boolean hasResultSet = statement.execute("SELECT * FROM root.ln.wf01.wt01");
      assertTrue(hasResultSet);
      int cnt;
      ArrayList<String> ans = new ArrayList<>();
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          ans.add(result);
          cnt++;
        }
      }
      for (int i = 0; i < ans.size(); i++) {
        assertEquals(exp[i], ans.get(i));
      }

      hasResultSet = statement.execute("SELECT  * FROM root.ln.wf01.wt01 WHERE \'status.2.3\' = false");
      assertTrue(hasResultSet);
      exp = new String[]{
              "1509465600002,false",
              "1509465600003,false"
      };
      ans = new ArrayList<>();
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          ans.add(result);
          cnt++;
        }
      }
      for (int i = 0; i < ans.size(); i++) {
        assertEquals(exp[i], ans.get(i));
      }
      statement.execute("DELETE FROM root.ln.wf01.wt01.\"status.2.3\" WHERE time < 1509465600001");
      statement.execute("DELETE TIMESERIES root.ln.wf01.wt01.\"status.2.3\"");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
