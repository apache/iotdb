/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.rest;

import com.alibaba.fastjson.JSONObject;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RestTest {

  private Client client = ClientBuilder.newClient();

  private static final String REST_URI
      = "http://localhost:8181/query";

  private static final String LOGIN
      = "http://localhost:8181/login";

  private static final String METRICS1
      = "http://localhost:8181/sql_arguments";

  private static final String METRICS2
      = "http://localhost:8181/server_information";

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

      statement.execute("select * from root");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery() {
    String json1 = "{\n"
        + "  \"range\": {\n"
        + "    \"from\": \"0\",\n"
        + "    \"to\": \"300\",\n"
        + "  },\n"
        + "  \n"
        + "  \"targets\": [\n"
        + "     { \"target\": \"root.ln.wf01.wt01.temperature\", \"type\": \"timeserie\" },\n"
        + "  ]\n"
        + "}";

    String json2 = "{username : \"root\", password : \"root\"}";
    client.target(LOGIN)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(JSONObject.parse(json2), MediaType.APPLICATION_JSON));
    Response response = client.target(REST_URI)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(JSONObject.parse(json1), MediaType.APPLICATION_JSON));
    String result = response.readEntity(String.class);
    Assert.assertEquals("[{\"datapoints\":[[1,\"1.1\"],[2,\"2.2\"],[3,\"3.3\"],[4,\"4.4\"],[5,\"5.5\"]],\"target\":\"root.ln.wf01.wt01.temperature\"}]"
        , result);
  }

  @Test
  public void testMetrics() {
    Response response1 = client.target(METRICS1).request(MediaType.APPLICATION_JSON).get();
    String result1 = response1.readEntity(String.class);
    System.out.println(result1);
    Response response2 = client.target(METRICS2).request(MediaType.APPLICATION_JSON).get();
    String result2 = response2.readEntity(String.class);
    System.out.println(result2);
  }
}
