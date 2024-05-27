/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.aggregation.maxby;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.utils.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.utils.constant.TestConstant.maxBy;
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBMaxByIT {
  protected static final String[] NON_ALIGNED_DATASET =
      new String[] {
        "CREATE DATABASE root.db",
        // x input
        "CREATE TIMESERIES root.db.d1.x1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x7 WITH DATATYPE=STRING, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x8 WITH DATATYPE=BLOB, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x9 WITH DATATYPE=DATE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.x10 WITH DATATYPE=TIMESTAMP, ENCODING=PLAIN",
        // y input
        "CREATE TIMESERIES root.db.d1.y1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y7 WITH DATATYPE=STRING, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y8 WITH DATATYPE=BLOB, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y9 WITH DATATYPE=DATE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.y10 WITH DATATYPE=TIMESTAMP, ENCODING=PLAIN",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) values(1, 1, 1, 1, 1, true, \"1\", '1', X'11', '2024-01-01', 1)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) values(2, 2, 2, 2, 2, false, \"2\", '2', X'22', '2024-01-02', 2)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) values(3, 3, 3, 3, 3, false, \"3\", '3', X'33', '2024-01-03', 3)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(2, 2, 2, 2, 2, true, \"4\", '2', X'22', '2024-01-02', 2)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(3, 3, 3, 3, 3, false, \"3\", '3', X'33', '2024-01-03', 3)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(4, 4, 4, 4, 4, false, \"4\", '4', X'44', '2024-01-04', 4)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,y7,y8,y9,y10) values(8, 3, 3, 3, 3, false, \"3\", '3', X'33', '2024-01-03', 3)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(8, 8, 8, 8, 8, false, \"4\", '8', X'44', '2024-01-08', 8)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,y7,y8,y9,y10) values(12, 3, 3, 3, 3, false, \"3\", '3', X'33', '2024-01-03', 3)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(12, 9, 9, 9, 9, false, \"4\", '9', X'44', '2024-01-09', 9)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,y7,y8,y9,y10) values(13, 4, 4, 4, 4, false, \"4\",'4', X'44', '2024-01-04', 4)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(13, 9, 9, 9, 9, false, \"4\",'9', X'44', '2024-01-09', 9)",
        "flush",

        // For Align By Device
        "CREATE TIMESERIES root.db.d2.x1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.x2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.x3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.x4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.x5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.x6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        // y input
        "CREATE TIMESERIES root.db.d2.y1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.y2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.y3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.y4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.y5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.y6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(1, 1, 1, 1, 1, true, \"1\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(2, 2, 2, 2, 2, false, \"2\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(3, 3, 3, 3, 3, false, \"3\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(2, 2, 2, 2, 2, true, \"4\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(3, 3, 3, 3, 3, false, \"3\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(4, 1, 1, 1, 1, false, \"1\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(12, 3, 3, 3, 3, false, \"3\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(12, 9, 9, 9, 9, false, \"1\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(13, 4, 4, 4, 4, false, \"4\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(13, 9, 9, 9, 9, false, \"1\")",
        "flush"
      };

  protected static final String UNSUPPORTED_TYPE_MESSAGE = "Unsupported data type in MaxBy/MinBy:";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(NON_ALIGNED_DATASET);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMaxByWithUnsupportedYInputTypes() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT max_by(x1, y5) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(UNSUPPORTED_TYPE_MESSAGE));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT max_by(x1, y6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(UNSUPPORTED_TYPE_MESSAGE));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT max_by(x5, y5) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(UNSUPPORTED_TYPE_MESSAGE));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT max_by(x5, y6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(UNSUPPORTED_TYPE_MESSAGE));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT max_by(x6, y5) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(UNSUPPORTED_TYPE_MESSAGE));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT max_by(x6, y6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(UNSUPPORTED_TYPE_MESSAGE));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT max_by(x6, y8) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(UNSUPPORTED_TYPE_MESSAGE));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithDifferentXAndYInputTypes() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Map<String, String[]> expectedHeaders =
          generateExpectedHeadersForMaxByTest(
              "root.db.d1",
              new String[] {"x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10"},
              new String[] {"y1", "y2", "y3", "y4", "y7", "y9", "y10"});
      String[] retArray =
          new String[] {"3,3,3.0,3.0,false,3,3,0x33,2024-01-03,1970-01-01T00:00:00.003Z,"};
      for (Map.Entry<String, String[]> expectedHeader : expectedHeaders.entrySet()) {
        String y = expectedHeader.getKey();
        resultSetEqualTest(
            String.format(
                "select max_by(x1,%s),max_by(x2,%s),max_by(x3,%s),max_by(x4,%s),max_by(x5,%s),max_by(x6,%s),max_by(x7,%s),max_by(x8,%s),max_by(x9,%s),max_by(x10,%s) from root.db.d1 where time <= 3",
                y, y, y, y, y, y, y, y, y, y),
            expectedHeader.getValue(),
            retArray);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithDifferentXAndYInputTypesAndNullXValue() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Map<String, String[]> expectedHeaders =
          generateExpectedHeadersForMaxByTest(
              "root.db.d1",
              new String[] {"x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10"},
              new String[] {"y1", "y2", "y3", "y4", "y7", "y9", "y10"});
      String[] retArray = new String[] {"null,null,null,null,null,null,null,null,null,null,"};
      for (Map.Entry<String, String[]> expectedHeader : expectedHeaders.entrySet()) {
        String y = expectedHeader.getKey();
        resultSetEqualTest(
            String.format(
                "select max_by(x1,%s),max_by(x2,%s),max_by(x3,%s),max_by(x4,%s),max_by(x5,%s),max_by(x6,%s),max_by(x7,%s),max_by(x8,%s),max_by(x9,%s),max_by(x10,%s) from root.db.d1 where time <= 4",
                y, y, y, y, y, y, y, y, y, y),
            expectedHeader.getValue(),
            retArray);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithDifferentYInputTypesAndXAsTime() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Map<String, String[]> expectedHeaders =
          generateExpectedHeadersForMaxByTest(
              "root.db.d1",
              new String[] {"Time", "Time", "Time", "Time", "Time", "Time"},
              new String[] {"y1", "y2", "y3", "y4"});
      String[] retArray = new String[] {"3,3,3,3,3,3,"};
      for (Map.Entry<String, String[]> expectedHeader : expectedHeaders.entrySet()) {
        String y = expectedHeader.getKey();
        resultSetEqualTest(
            String.format(
                "select max_by(time,%s),max_by(time,%s),max_by(time,%s),max_by(time,%s),max_by(time,%s),max_by(time,%s) from root.db.d1 where time <= 3",
                y, y, y, y, y, y),
            expectedHeader.getValue(),
            retArray);
      }
      String[] retArray1 = new String[] {"4,4,4,4,4,4,"};
      for (Map.Entry<String, String[]> expectedHeader : expectedHeaders.entrySet()) {
        String y = expectedHeader.getKey();
        resultSetEqualTest(
            String.format(
                "select max_by(time,%s),max_by(time,%s),max_by(time,%s),max_by(time,%s),max_by(time,%s),max_by(time,%s) from root.db.d1 where time <= 4",
                y, y, y, y, y, y),
            expectedHeader.getValue(),
            retArray1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithExpression() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] expectedHeader =
          new String[] {
            "max_by(root.db.d1.x1 + 1 - 3, -cos(sin(root.db.d1.y2 / 10)))",
            "max_by(root.db.d1.x2 * 2 / 3, -cos(sin(root.db.d1.y2 / 10)))",
            "max_by(floor(root.db.d1.x3), -cos(sin(root.db.d1.y2 / 10)))",
            "max_by(ceil(root.db.d1.x4), -cos(sin(root.db.d1.y2 / 10)))",
            "max_by(root.db.d1.x5, -cos(sin(root.db.d1.y2 / 10)))",
            "max_by(REPLACE(root.db.d1.x6, '3', '4'), -cos(sin(root.db.d1.y2 / 10)))",
          };
      String[] retArray = new String[] {"1.0,2.0,3.0,3.0,false,4,"};
      String y = "-cos(sin(y2 / 10))";
      resultSetEqualTest(
          String.format(
              "select max_by(x1 + 1 - 3,%s),max_by(x2 * 2 / 3,%s),max_by(floor(x3),%s),max_by(ceil(x4),%s),max_by(x5,%s),max_by(replace(x6, '3', '4'),%s) from root.db.d1 where time <= 3",
              y, y, y, y, y, y),
          expectedHeader,
          retArray);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithAlignByDevice() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] expectedHeader =
          new String[] {
            DEVICE,
            "max_by(x1 + 1 - 3, -cos(sin(y2 / 10)))",
            "max_by(x2 * 2 / 3, -cos(sin(y2 / 10)))",
            "max_by(floor(x3), -cos(sin(y2 / 10)))",
            "max_by(ceil(x4), -cos(sin(y2 / 10)))",
            "max_by(x5, -cos(sin(y2 / 10)))",
            "max_by(REPLACE(x6, '3', '4'), -cos(sin(y2 / 10)))",
          };
      String[] retArray =
          new String[] {
            "root.db.d1,1.0,2.0,3.0,3.0,false,4,", "root.db.d2,1.0,2.0,3.0,3.0,false,4,"
          };
      String y = "-cos(sin(y2 / 10))";
      resultSetEqualTest(
          String.format(
              "select max_by(x1 + 1 - 3,%s),max_by(x2 * 2 / 3,%s),max_by(floor(x3),%s),max_by(ceil(x4),%s),max_by(x5,%s),max_by(replace(x6, '3', '4'),%s) from root.db.** where time <= 3 align by device",
              y, y, y, y, y, y),
          expectedHeader,
          retArray);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithGroupByTime() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] expectedHeader =
          new String[] {
            TIMESTAMP_STR,
            "max_by(root.db.d1.x1, root.db.d1.y2)",
            "max_by(root.db.d1.x2, root.db.d1.y2)",
            "max_by(root.db.d1.x3, root.db.d1.y2)",
            "max_by(root.db.d1.x4, root.db.d1.y2)",
            "max_by(root.db.d1.x5, root.db.d1.y2)",
            "max_by(root.db.d1.x6, root.db.d1.y2)",
          };
      String y = "y2";
      // order by time ASC
      String[] retArray1 =
          new String[] {
            "0,3,3,3.0,3.0,false,3,", "4,null,null,null,null,null,null,", "8,3,3,3.0,3.0,false,3,"
          };
      resultSetEqualTest(
          String.format(
              "select max_by(x1,%s),max_by(x2,%s),max_by(x3,%s),max_by(x4,%s),max_by(x5,%s),max_by(x6,%s) from root.db.d1 where time <= 10 group by ([0,9),4ms) ",
              y, y, y, y, y, y),
          expectedHeader,
          retArray1);

      // order by time DESC
      String[] retArray2 =
          new String[] {
            "12,3,3,3.0,3.0,false,3,",
            "8,3,3,3.0,3.0,false,3,",
            "4,null,null,null,null,null,null,",
            "0,3,3,3.0,3.0,false,3,"
          };
      resultSetEqualTest(
          String.format(
              "select max_by(x1,%s),max_by(x2,%s),max_by(x3,%s),max_by(x4,%s),max_by(x5,%s),max_by(x6,%s) from root.db.d1 group by ([0,14),4ms) order by time desc",
              y, y, y, y, y, y),
          expectedHeader,
          retArray2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithSlidingWindow() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] retArray =
          new String[] {
            "0,3,3,3.0,3.0,false,3,",
            "2,null,null,null,null,null,null,",
            "4,null,null,null,null,null,null,",
            "6,3,3,3.0,3.0,false,3,",
            "8,3,3,3.0,3.0,false,3,"
          };
      String[] yArray = new String[] {"y1", "y2", "y3", "y4"};
      for (String y : yArray) {
        String[] expectedHeader =
            new String[] {
              TIMESTAMP_STR,
              String.format("max_by(root.db.d1.x1, root.db.d1.%s)", y),
              String.format("max_by(root.db.d1.x2, root.db.d1.%s)", y),
              String.format("max_by(root.db.d1.x3, root.db.d1.%s)", y),
              String.format("max_by(root.db.d1.x4, root.db.d1.%s)", y),
              String.format("max_by(root.db.d1.x5, root.db.d1.%s)", y),
              String.format("max_by(root.db.d1.x6, root.db.d1.%s)", y),
            };
        resultSetEqualTest(
            String.format(
                "select max_by(x1,%s),max_by(x2,%s),max_by(x3,%s),max_by(x4,%s),max_by(x5,%s),max_by(x6,%s) from root.db.d1 where time <= 10 group by ([0,9),4ms,2ms) ",
                y, y, y, y, y, y),
            expectedHeader,
            retArray);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithHaving() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] expectedHeader =
          new String[] {
            TIMESTAMP_STR,
            "max_by(root.db.d1.x1, root.db.d1.y2)",
            "max_by(root.db.d1.x2, root.db.d1.y2)",
            "max_by(root.db.d1.x3, root.db.d1.y2)",
            "max_by(root.db.d1.x4, root.db.d1.y2)",
            "max_by(root.db.d1.x5, root.db.d1.y2)",
            "max_by(root.db.d1.x6, root.db.d1.y2)",
          };
      String[] retArray = new String[] {"8,3,3,3.0,3.0,false,3,"};
      String y = "y2";
      resultSetEqualTest(
          String.format(
              "select max_by(x1,%s),max_by(x2,%s),max_by(x3,%s),max_by(x4,%s),max_by(x5,%s),max_by(x6,%s) from root.db.d1 group by ([0,9),4ms) having max_by(time, %s) > 4",
              y, y, y, y, y, y, y),
          expectedHeader,
          retArray);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxByWithGroupByLevel() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] retArray = new String[] {"3,3,3.0,3.0,false,3,"};
      String[] yArray = new String[] {"y1", "y2", "y3", "y4"};
      for (String y : yArray) {
        String[] expectedHeader =
            new String[] {
              String.format("max_by(root.*.*.x1, root.*.*.%s)", y),
              String.format("max_by(root.*.*.x2, root.*.*.%s)", y),
              String.format("max_by(root.*.*.x3, root.*.*.%s)", y),
              String.format("max_by(root.*.*.x4, root.*.*.%s)", y),
              String.format("max_by(root.*.*.x5, root.*.*.%s)", y),
              String.format("max_by(root.*.*.x6, root.*.*.%s)", y),
            };
        resultSetEqualTest(
            String.format(
                "select max_by(x1,%s),max_by(x2,%s),max_by(x3,%s),max_by(x4,%s),max_by(x5,%s),max_by(x6,%s) from root.db.** group by level = 0",
                y, y, y, y, y, y),
            expectedHeader,
            retArray);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * @return yInput -> expectedHeader
   */
  private Map<String, String[]> generateExpectedHeadersForMaxByTest(
      String device, String[] xInput, String[] yInput) {
    Map<String, String[]> res = new HashMap<>();
    Arrays.stream(yInput)
        .forEach(
            y -> {
              res.put(
                  y,
                  Arrays.stream(xInput)
                      .map(x -> maxBy("Time".equals(x) ? x : device + "." + x, device + "." + y))
                      .toArray(String[]::new));
            });
    return res;
  }
}
