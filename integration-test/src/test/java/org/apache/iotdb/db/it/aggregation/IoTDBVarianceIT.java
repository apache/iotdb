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

package org.apache.iotdb.db.it.aggregation;

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

import static org.apache.iotdb.db.it.utils.TestUtils.*;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.*;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBVarianceIT {
  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.db",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s5 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        // for group by level use
        "CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s5 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(1, 1, 1, true, 1, 1, \"1\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(2, 2, 2, false, 2, 2, \"2\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(3, 3, 2, false, 3, 2, \"2\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(10000000000, 4, 1, true, 4, 1, \"1\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(10000000001, 5, 1, true, 5, 1, \"1\")",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(1, 1, 2, 3, 4)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(2, 1, 2, 3, 4)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(10000000000, 1, 2, 3, 4)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(10000000001, 1, 2, 3, 4)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(10000000002, 1, 2, 3, 4)",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAllVarianceWithUnsupportedTypes() {
    String errorMsg =
        "Aggregate functions [AVG, SUM, EXTREME, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT stddev(s3) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT stddev(s6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT stddev_pop(s3) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT stddev_pop(s6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT stddev_samp(s3) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT stddev_samp(s6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT variance(s3) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT variance(s6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT var_pop(s3) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT var_pop(s6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT var_samp(s3) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT var_samp(s6) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(errorMsg));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testStddevWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {
          stddev("root.db.d1.s1"),
          stddev("root.db.d1.s2"),
          stddev("root.db.d1.s4"),
          stddev("root.db.d1.s5"),
        };
    String[] retArray =
        new String[] {
          "1.5811388300841898,0.5477225575051661,1.5811388300841898,0.5477225575051661,"
        };
    resultSetEqualTest(
        "select stddev(s1),stddev(s2),stddev(s4),stddev(s5) from root.db.d1",
        expectedHeader,
        retArray);

    retArray = new String[] {"1.0,0.5773502691896257,1.0,0.5773502691896257,"};
    resultSetEqualTest(
        "select stddev(s1),stddev(s2),stddev(s4),stddev(s5) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevPopWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {
          stddevPop("root.db.d1.s1"),
          stddevPop("root.db.d1.s2"),
          stddevPop("root.db.d1.s4"),
          stddevPop("root.db.d1.s5"),
        };
    String[] retArray =
        new String[] {
          "1.4142135623730951,0.4898979485566356,1.4142135623730951,0.4898979485566356,"
        };
    resultSetEqualTest(
        "select stddev_pop(s1),stddev_pop(s2),stddev_pop(s4),stddev_pop(s5) from root.db.d1",
        expectedHeader,
        retArray);

    retArray =
        new String[] {"0.816496580927726,0.4714045207910317,0.816496580927726,0.4714045207910317,"};
    resultSetEqualTest(
        "select stddev_pop(s1),stddev_pop(s2),stddev_pop(s4),stddev_pop(s5) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevSampWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {
          stddevSamp("root.db.d1.s1"),
          stddevSamp("root.db.d1.s2"),
          stddevSamp("root.db.d1.s4"),
          stddevSamp("root.db.d1.s5"),
        };
    String[] retArray =
        new String[] {
          "1.5811388300841898,0.5477225575051661,1.5811388300841898,0.5477225575051661,"
        };
    resultSetEqualTest(
        "select stddev_samp(s1),stddev_samp(s2),stddev_samp(s4),stddev_samp(s5) from root.db.d1",
        expectedHeader,
        retArray);

    retArray = new String[] {"1.0,0.5773502691896257,1.0,0.5773502691896257,"};
    resultSetEqualTest(
        "select stddev_samp(s1),stddev_samp(s2),stddev_samp(s4),stddev_samp(s5) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarianceWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {
          variance("root.db.d1.s1"),
          variance("root.db.d1.s2"),
          variance("root.db.d1.s4"),
          variance("root.db.d1.s5"),
        };
    String[] retArray = new String[] {"2.5,0.3,2.5,0.3,"};
    resultSetEqualTest(
        "select variance(s1),variance(s2),variance(s4),variance(s5) from root.db.d1",
        expectedHeader,
        retArray);

    retArray = new String[] {"1.0,0.3333333333333333,1.0,0.3333333333333333,"};
    resultSetEqualTest(
        "select variance(s1),variance(s2),variance(s4),variance(s5) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarPopWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {
          varPop("root.db.d1.s1"),
          varPop("root.db.d1.s2"),
          varPop("root.db.d1.s4"),
          varPop("root.db.d1.s5"),
        };
    String[] retArray = new String[] {"2.0,0.24,2.0,0.24,"};
    resultSetEqualTest(
        "select var_pop(s1),var_pop(s2),var_pop(s4),var_pop(s5) from root.db.d1",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "0.6666666666666666,0.2222222222222222,0.6666666666666666,0.2222222222222222,"
        };
    resultSetEqualTest(
        "select var_pop(s1),var_pop(s2),var_pop(s4),var_pop(s5) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarSampWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {
          varSamp("root.db.d1.s1"),
          varSamp("root.db.d1.s2"),
          varSamp("root.db.d1.s4"),
          varSamp("root.db.d1.s5"),
        };
    String[] retArray = new String[] {"2.5,0.3,2.5,0.3,"};
    resultSetEqualTest(
        "select var_samp(s1),var_samp(s2),var_samp(s4),var_samp(s5) from root.db.d1",
        expectedHeader,
        retArray);

    retArray = new String[] {"1.0,0.3333333333333333,1.0,0.3333333333333333,"};
    resultSetEqualTest(
        "select var_samp(s1),var_samp(s2),var_samp(s4),var_samp(s5) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE, stddev("s1"), stddev("s2"), stddev("s4"), stddev("s5"),
        };
    String[] retArray =
        new String[] {
          "root.db.d1,1.5811388300841898,0.5477225575051661,1.5811388300841898,0.5477225575051661,"
        };
    resultSetEqualTest(
        "select stddev(s1),stddev(s2),stddev(s4),stddev(s5) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    retArray = new String[] {"root.db.d1,1.0,0.5773502691896257,1.0,0.5773502691896257,"};
    resultSetEqualTest(
        "select stddev(s1),stddev(s2),stddev(s4),stddev(s5) from root.db.d1 where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevPopAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE, stddevPop("s1"), stddevPop("s2"), stddevPop("s4"), stddevPop("s5"),
        };
    String[] retArray =
        new String[] {
          "root.db.d1,1.4142135623730951,0.4898979485566356,1.4142135623730951,0.4898979485566356,"
        };
    resultSetEqualTest(
        "select stddev_pop(s1),stddev_pop(s2),stddev_pop(s4),stddev_pop(s5) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "root.db.d1,0.816496580927726,0.4714045207910317,0.816496580927726,0.4714045207910317,"
        };
    resultSetEqualTest(
        "select stddev_pop(s1),stddev_pop(s2),stddev_pop(s4),stddev_pop(s5) from root.db.d1 where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevSampAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE, stddevSamp("s1"), stddevSamp("s2"), stddevSamp("s4"), stddevSamp("s5"),
        };
    String[] retArray =
        new String[] {
          "root.db.d1,1.5811388300841898,0.5477225575051661,1.5811388300841898,0.5477225575051661,"
        };
    resultSetEqualTest(
        "select stddev_samp(s1),stddev_samp(s2),stddev_samp(s4),stddev_samp(s5) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    retArray = new String[] {"root.db.d1,1.0,0.5773502691896257,1.0,0.5773502691896257,"};
    resultSetEqualTest(
        "select stddev_samp(s1),stddev_samp(s2),stddev_samp(s4),stddev_samp(s5) from root.db.d1 where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarianceAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE, variance("s1"), variance("s2"), variance("s4"), variance("s5"),
        };
    String[] retArray = new String[] {"root.db.d1,2.5,0.3,2.5,0.3,"};
    resultSetEqualTest(
        "select variance(s1),variance(s2),variance(s4),variance(s5) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    retArray = new String[] {"root.db.d1,1.0,0.3333333333333333,1.0,0.3333333333333333,"};
    resultSetEqualTest(
        "select variance(s1),variance(s2),variance(s4),variance(s5) from root.db.d1 where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarPopAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE, varPop("s1"), varPop("s2"), varPop("s4"), varPop("s5"),
        };
    String[] retArray = new String[] {"root.db.d1,2.0,0.24,2.0,0.24,"};
    resultSetEqualTest(
        "select var_pop(s1),var_pop(s2),var_pop(s4),var_pop(s5) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "root.db.d1,0.6666666666666666,0.2222222222222222,0.6666666666666666,0.2222222222222222,"
        };
    resultSetEqualTest(
        "select var_pop(s1),var_pop(s2),var_pop(s4),var_pop(s5) from root.db.d1 where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarSampAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE, varSamp("s1"), varSamp("s2"), varSamp("s4"), varSamp("s5"),
        };
    String[] retArray = new String[] {"root.db.d1,2.5,0.3,2.5,0.3,"};
    resultSetEqualTest(
        "select var_samp(s1),var_samp(s2),var_samp(s4),var_samp(s5) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    retArray = new String[] {"root.db.d1,1.0,0.3333333333333333,1.0,0.3333333333333333,"};
    resultSetEqualTest(
        "select var_samp(s1),var_samp(s2),var_samp(s4),var_samp(s5) from root.db.d1 where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevInHaving() {
    String[] expectedHeader = new String[] {stddev("root.db.d1.s1")};
    String[] retArray = new String[] {"1.5811388300841898,"};
    resultSetEqualTest(
        "select stddev(s1) from root.db.d1 having stddev(s2)>0", expectedHeader, retArray);
  }

  @Test
  public void testStddevPopInHaving() {
    String[] expectedHeader = new String[] {stddevPop("root.db.d1.s1")};
    String[] retArray = new String[] {"1.4142135623730951,"};
    resultSetEqualTest(
        "select stddev_pop(s1) from root.db.d1 having stddev_pop(s2)>0", expectedHeader, retArray);
  }

  @Test
  public void testStddevSampInHaving() {
    String[] expectedHeader = new String[] {stddevSamp("root.db.d1.s1")};
    String[] retArray = new String[] {"1.5811388300841898,"};
    resultSetEqualTest(
        "select stddev_samp(s1) from root.db.d1 having stddev_samp(s2)>0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarianceInHaving() {
    String[] expectedHeader = new String[] {variance("root.db.d1.s1")};
    String[] retArray = new String[] {"2.5,"};
    resultSetEqualTest(
        "select variance(s1) from root.db.d1 having variance(s2)>0", expectedHeader, retArray);
  }

  @Test
  public void testVarPopInHaving() {
    String[] expectedHeader = new String[] {varPop("root.db.d1.s1")};
    String[] retArray = new String[] {"2.0,"};
    resultSetEqualTest(
        "select var_pop(s1) from root.db.d1 having var_pop(s2)>0", expectedHeader, retArray);
  }

  @Test
  public void testVarSampInHaving() {
    String[] expectedHeader = new String[] {varSamp("root.db.d1.s1")};
    String[] retArray = new String[] {"2.5,"};
    resultSetEqualTest(
        "select var_samp(s1) from root.db.d1 having var_samp(s2)>0", expectedHeader, retArray);
  }

  @Test
  public void testStddevWithGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          stddev("root.*.*.s1"),
          stddev("root.*.*.s2"),
          stddev("root.*.*.s4"),
          stddev("root.*.*.s5"),
        };
    String[] retArray =
        new String[] {
          "1.4907119849998598,0.48304589153964794,1.0540925533894598,1.4181364924121764,"
        };
    resultSetEqualTest(
        "select stddev(s1),stddev(s2),stddev(s4),stddev(s5) from root.db.* group by level = 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevPopWithGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          stddevPop("root.*.*.s1"),
          stddevPop("root.*.*.s2"),
          stddevPop("root.*.*.s4"),
          stddevPop("root.*.*.s5"),
        };
    String[] retArray =
        new String[] {"1.4142135623730951,0.45825756949558405,1.0,1.3453624047073711,"};
    resultSetEqualTest(
        "select stddev_pop(s1),stddev_pop(s2),stddev_pop(s4),stddev_pop(s5) from root.db.* group by level = 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevSampWithGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          stddevSamp("root.*.*.s1"),
          stddevSamp("root.*.*.s2"),
          stddevSamp("root.*.*.s4"),
          stddevSamp("root.*.*.s5"),
        };
    String[] retArray =
        new String[] {
          "1.4907119849998598,0.48304589153964794,1.0540925533894598,1.4181364924121764,"
        };
    resultSetEqualTest(
        "select stddev_samp(s1),stddev_samp(s2),stddev_samp(s4),stddev_samp(s5) from root.db.* group by level = 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarianceWithGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          variance("root.*.*.s1"),
          variance("root.*.*.s2"),
          variance("root.*.*.s4"),
          variance("root.*.*.s5"),
        };
    String[] retArray =
        new String[] {
          "2.2222222222222223,0.23333333333333334,1.1111111111111112,2.011111111111111,"
        };
    resultSetEqualTest(
        "select variance(s1),variance(s2),variance(s4),variance(s5) from root.db.* group by level = 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarPopWithGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          varPop("root.*.*.s1"),
          varPop("root.*.*.s2"),
          varPop("root.*.*.s4"),
          varPop("root.*.*.s5"),
        };
    String[] retArray = new String[] {"2.0,0.21000000000000002,1.0,1.81,"};
    resultSetEqualTest(
        "select var_pop(s1),var_pop(s2),var_pop(s4),var_pop(s5) from root.db.* group by level = 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarSampWithGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          varSamp("root.*.*.s1"),
          varSamp("root.*.*.s2"),
          varSamp("root.*.*.s4"),
          varSamp("root.*.*.s5"),
        };
    String[] retArray =
        new String[] {
          "2.2222222222222223,0.23333333333333334,1.1111111111111112,2.011111111111111,"
        };
    resultSetEqualTest(
        "select var_samp(s1),var_samp(s2),var_samp(s4),var_samp(s5) from root.db.* group by level = 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevWithSlidingWindow() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, stddev("root.db.d1.s1")};
    String[] retArray = new String[] {"1,1.0,", "3,null,"};
    resultSetEqualTest(
        "select stddev(s1) from root.db.d1 group by time([1,4),3ms,2ms)", expectedHeader, retArray);
  }

  @Test
  public void testStddevPopWithSlidingWindow() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, stddevPop("root.db.d1.s1")};
    String[] retArray = new String[] {"1,0.816496580927726,", "3,0.0,"};
    resultSetEqualTest(
        "select stddev_pop(s1) from root.db.d1 group by time([1,4),3ms,2ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testStddevSampWithSlidingWindow() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, stddevSamp("root.db.d1.s1")};
    String[] retArray = new String[] {"1,1.0,", "3,null,"};
    resultSetEqualTest(
        "select stddev_samp(s1) from root.db.d1 group by time([1,4),3ms,2ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarianceWithSlidingWindow() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, variance("root.db.d1.s1")};
    String[] retArray = new String[] {"1,1.0,", "3,null,"};
    resultSetEqualTest(
        "select variance(s1) from root.db.d1 group by time([1,4),3ms,2ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarPopWithSlidingWindow() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, varPop("root.db.d1.s1")};
    String[] retArray = new String[] {"1,0.6666666666666666,", "3,0.0,"};
    resultSetEqualTest(
        "select var_pop(s1) from root.db.d1 group by time([1,4),3ms,2ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testVarSampWithSlidingWindow() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, varSamp("root.db.d1.s1")};
    String[] retArray = new String[] {"1,1.0,", "3,null,"};
    resultSetEqualTest(
        "select var_samp(s1) from root.db.d1 group by time([1,4),3ms,2ms)",
        expectedHeader,
        retArray);
  }
}
