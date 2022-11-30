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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;
import static org.apache.iotdb.itbase.constant.TestConstant.count;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAggregationOptimizeIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.test",
        "CREATE TIMESERIES root.test.1region_d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.1region_d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.1region_d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.1region_d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.2region_d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.2region_d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.2region_d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.2region_d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.test.1region_d1(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test.1region_d2(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test.1region_d1(timestamp,s1,s2) values(2, 2, 2)",
        "INSERT INTO root.test.1region_d2(timestamp,s1,s2) values(2, 2, 2)",
        "INSERT INTO root.test.2region_d1(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test.2region_d2(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test.2region_d1(timestamp,s1,s2) values(1000000000000, 1, 1)",
        "INSERT INTO root.test.2region_d2(timestamp,s1,s2) values(1000000000000, 1, 1)",
        "flush",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testEachSeriesOneRegion() {
    String[] expectedHeader =
        new String[] {
          count("root.test.1region_d1.s1"),
          count("root.test.1region_d2.s1"),
          count("root.test.1region_d1.s2"),
          count("root.test.1region_d2.s2")
        };
    String[] retArray = new String[] {"2,2,2,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1), count(s2) from root.test.1region_d1, root.test.1region_d2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testSomeSeriesOneRegion() {
    String[] expectedHeader =
        new String[] {count("root.test.1region_d1.s1"), count("root.test.2region_d2.s1")};
    String[] retArray = new String[] {"2,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1) from root.test.1region_d1, root.test.2region_d2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAggregationResultConcatOneRegion() {
    String[] expectedHeader =
        new String[] {count("root.test.2region_d1.s1"), count("root.test.2region_d1.s2")};
    String[] retArray = new String[] {"2,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1),count(s2) from root.test.2region_d1", expectedHeader, retArray);
  }

  @Test
  public void mixTest() {
    String[] expectedHeader =
        new String[] {
          count("root.test.2region_d1.s1"),
          count("root.test.2region_d2.s1"),
          count("root.test.1region_d2.s1"),
          count("root.test.1region_d1.s1"),
          count("root.test.2region_d1.s2"),
          count("root.test.2region_d2.s2"),
          count("root.test.1region_d2.s2"),
          count("root.test.1region_d1.s2")
        };
    String[] retArray = new String[] {"2,2,2,2,2,2,2,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1),count(s2) from root.test.**", expectedHeader, retArray);
  }
}
