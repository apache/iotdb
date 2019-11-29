package org.apache.iotdb.db.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBLoadExternalTsfileTest {

  private static IoTDB daemon;
  private static String[] sqls = new String[]{
      "SET STORAGE GROUP TO root.vehicle",
      "SET STORAGE GROUP TO root.test",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "insert into root.vehicle.d0(timestamp,s0) values(10,100)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(12,101,'102')",
      "insert into root.vehicle.d0(timestamp,s1) values(19,'103')",
      "insert into root.vehicle.d1(timestamp,s2) values(11,104.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(15,105.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(17,false)",
      "insert into root.vehicle.d0(timestamp,s0) values(20,1000)"
  };

  private static final String TIMESTAMP_STR = "Time";
  private static final String TEMPERATURE_STR = "root.ln.wf01.wt01.temperature";
  private static final String STATUS_STR = "root.ln.wf01.wt01.status";
  private static final String HARDWARE_STR = "root.ln.wf01.wt01.hardware";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void LoadNewTsfileTest() throws SQLException {
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("flush");
      List<TsFileResource> resources = StorageEngine.getInstance().getProcessor("root.vehicle")
          .getSequenceFileList();
      for(TsFileResource resource:resources){
        System.out.println(resource.getFile().getAbsolutePath());
      }
    } catch (StorageEngineException e) {
      e.printStackTrace();
    }
  }

  private void prepareData() throws SQLException {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {


      for (String sql : sqls) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

