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

package org.apache.iotdb.db.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBEncodingIT {

  private static final String[] databasesToClear =
      new String[] {"root.db_0", "root.db1", "root.turbine1"};

  @BeforeClass
  public static void setUpClass() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void tearDown() {
    for (String database : databasesToClear) {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement("DELETE DATABASE " + database);
      } catch (Exception ignored) {

      }
    }
  }

  @Test
  public void testSetEncodingRegularFailed() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.test1.s0 WITH DATATYPE=INT64,ENCODING=REGULAR");
      fail();
    } catch (SQLException e) {
      assertEquals(TSStatusCode.METADATA_ERROR.getStatusCode(), e.getErrorCode());
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderTS_2DIFF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=TS_2DIFF");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(3,1300)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,1400)");

      statement.execute("flush");

      int[] result = new int[] {1100, 1200, 1300, 1400};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderTS_2DIFFOutofOrder() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=TS_2DIFF");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(7,1000)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,2200)");
      statement.execute("flush");

      int[] result = new int[] {1200, 1100, 2200, 1000};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderRLE() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=RLE");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(3,1300)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,1400)");
      statement.execute("flush");

      int[] result = new int[] {1100, 1200, 1300, 1400};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderRLEOutofOrder() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=RLE");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(7,1000)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,2200)");
      statement.execute("flush");

      int[] result = new int[] {1200, 1100, 2200, 1000};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderGORILLA() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=GORILLA");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(3,1300)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,1400)");
      statement.execute("flush");

      int[] result = new int[] {1100, 1200, 1300, 1400};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderGORILLAOutofOrder() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=GORILLA");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(7,1000)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,2200)");
      statement.execute("flush");

      int[] result = new int[] {1200, 1100, 2200, 1000};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderZIGZAG() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=ZIGZAG");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(3,1300)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,1400)");
      statement.execute("flush");

      int[] result = new int[] {1100, 1200, 1300, 1400};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderZIGZAGOutofOrder() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=ZIGZAG");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(7,1000)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,2200)");
      statement.execute("flush");

      int[] result = new int[] {1200, 1100, 2200, 1000};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderDictionary() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.city WITH DATATYPE=TEXT,ENCODING=DICTIONARY");
      statement.execute("insert into root.db_0.tab0(time,city) values(1,\"Nanjing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(2,\"Nanjing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(3,\"Beijing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(4,\"Shanghai\")");
      statement.execute("flush");

      String[] result = new String[] {"Nanjing", "Nanjing", "Beijing", "Shanghai"};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          String city = resultSet.getString("root.db_0.tab0.city");
          assertEquals(result[index], city);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderDictionaryOutOfOrder() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.city WITH DATATYPE=TEXT,ENCODING=DICTIONARY");
      statement.execute("insert into root.db_0.tab0(time,city) values(1,\"Nanjing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(2,\"Nanjing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(4,\"Beijing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(3,\"Shanghai\")");
      statement.execute("flush");

      String[] result = new String[] {"Nanjing", "Nanjing", "Shanghai", "Beijing"};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          String city = resultSet.getString("root.db_0.tab0.city");
          assertEquals(result[index], city);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  public double SNR(int[] gd, int[] x, int length) {
    double noise_power = 0, signal_power = 0;
    for (int i = 0; i < length; i++) {
      noise_power += (gd[i] - x[i]) * (gd[i] - x[i]);
      signal_power += gd[i] * gd[i];
    }
    if (noise_power == 0) {
      return Double.POSITIVE_INFINITY;
    } else {
      return 10 * Math.log10(signal_power / noise_power);
    }
  }

  @Test
  public void testDoublePrecision1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.turbine1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=DOUBLE, encoding=PLAIN, compression=SNAPPY");

      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1.2345678)");

      ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**");

      String str = "1.2345678";
      while (resultSet.next()) {
        assertEquals(str, resultSet.getString("root.turbine1.d1.s1"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testDoublePrecision2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.turbine1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=DOUBLE, encoding=RLE, compression=SNAPPY");

      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1.2345678)");

      ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**");

      String str = "1.23";
      while (resultSet.next()) {
        assertEquals(str, resultSet.getString("root.turbine1.d1.s1"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testFloatPrecision1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.turbine1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=FLOAT, encoding=PLAIN, compression=SNAPPY");

      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1.2345678)");

      ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**");

      String str = "1.2345678";
      while (resultSet.next()) {
        assertEquals(str, resultSet.getString("root.turbine1.d1.s1"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testFloatPrecision2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.turbine1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");

      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1.2345678)");

      ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**");

      String str = "1.23";
      while (resultSet.next()) {
        assertEquals(str, resultSet.getString("root.turbine1.d1.s1"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCreateNewTypes() throws Exception {
    String currDB = "root.db1";
    int seriesCnt = 0;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.STRING, TSDataType.BLOB, TSDataType.TIMESTAMP, TSDataType.DATE
        };

    // supported encodings
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (TSDataType dataType : dataTypes) {
        for (TSEncoding encoding : TSEncoding.values()) {
          if (encoding.isSupported(dataType)) {
            statement.execute(
                "create timeseries "
                    + currDB
                    + ".d1.s"
                    + seriesCnt
                    + " with datatype="
                    + dataType
                    + ", encoding="
                    + encoding
                    + ", compression=SNAPPY");
            seriesCnt++;
          }
        }
      }

      ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES");

      while (resultSet.next()) {
        seriesCnt--;
      }
      assertEquals(0, seriesCnt);
      statement.execute("DROP DATABASE " + currDB);
    }

    // unsupported encodings
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (TSDataType dataType : dataTypes) {
        for (TSEncoding encoding : TSEncoding.values()) {
          if (!encoding.isSupported(dataType)) {
            try {
              statement.execute(
                  "create timeseries "
                      + currDB
                      + ".d1.s"
                      + seriesCnt
                      + " with datatype="
                      + dataType
                      + ", encoding="
                      + encoding
                      + ", compression=SNAPPY");
              fail("Should have thrown an exception");
            } catch (SQLException e) {
              assertEquals(
                  "507: encoding " + encoding + " does not support " + dataType, e.getMessage());
            }
            seriesCnt++;
          }
        }
      }

      ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES");
      assertFalse(resultSet.next());
    }
  }
}
