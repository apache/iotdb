package org.apache.iotdb.db.rest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Locale;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RestTest {
  private static final String REST_URI
      = "http://localhost:8181/rest/query";

  private static String[] creationSqls = new String[]{
      "SET STORAGE GROUP TO root.vehicle.d0",
      "SET STORAGE GROUP TO root.vehicle.d1",

      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
  };
  private static String[] dataSet2 = new String[]{
      "SET STORAGE GROUP TO root.ln.wf01.wt01",
      "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(1, 1.1, false, 11)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(2, 2.2, true, 22)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(3, 3.3, false, 33 )",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(4, 4.4, false, 44)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(5, 5.5, false, 55)"
  };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(1000);
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(86400);
    EnvironmentUtils.cleanEnv();
  }

  private void prepareData() {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      for (String sql : dataSet2) {
        statement.execute(sql);
      }

      // prepare BufferWrite file
      String insertTemplate = "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)"
          + " VALUES(%d,%d,%d,%f,%s,%s)";
      for (int i = 5000; i < 7000; i++) {
        statement.execute(String
            .format(Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "\'" + i + "\'", "true"));
      }
      statement.execute("flush");
      for (int i = 7500; i < 8500; i++) {
        statement.execute(String
            .format(Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "\'" + i + "\'", "false"));
      }
      statement.execute("flush");
      // prepare Unseq-File
      for (int i = 500; i < 1500; i++) {
        statement.execute(String
            .format(Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "\'" + i + "\'", "true"));
      }
      statement.execute("flush");
      for (int i = 3000; i < 6500; i++) {
        statement.execute(String
            .format(Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "\'" + i + "\'", "false"));
      }
      statement.execute("merge");

      // prepare BufferWrite cache
      for (int i = 9000; i < 10000; i++) {
        statement.execute(String
            .format(Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "\'" + i + "\'", "true"));
      }
      // prepare Overflow cache
      for (int i = 2000; i < 2500; i++) {
        statement.execute(String
            .format(Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "\'" + i + "\'", "false"));
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery() {
    Client client = ClientBuilder.newClient();
    String json = "{\n"
        + "  \"range\": {\n"
        + "    \"from\": \"1\",\n"
        + "    \"to\": \"300\",\n"
        + "  },\n"
        + "  \n"
        + "  \"targets\": [\n"
        + "     { \"target\": \"root.ln.wf01.wt01\", \"type\": \"timeserie\" },\n"
        + "  ]\n"
        + "}";
    Response response = client.target(REST_URI)
        .request(MediaType.TEXT_PLAIN)
        .post(Entity.entity(json, MediaType.TEXT_PLAIN));
    String result = response.readEntity(String.class);
    System.out.println(result);
  }
}
