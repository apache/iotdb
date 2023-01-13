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

package org.apache.iotdb.db.it.builtinfunction.scalar;

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
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDiffFunctionIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.test",
        "CREATE TIMESERIES root.test.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.s2 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "INSERT INTO root.test(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test(timestamp,s1) values(2, 2)",
        "INSERT INTO root.test(timestamp,s2) values(3, 3)",
        "INSERT INTO root.test(timestamp,s1) values(4, 4)",
        "INSERT INTO root.test(timestamp,s1,s2) values(5, 5, 5)",
        "INSERT INTO root.test(timestamp,s2) values(6, 6)",
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
  public void testNewTransformerIgnoreNull() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, "Diff(root.test.s1)", "diff(root.test.s2)"};
    String[] retArray =
        new String[] {
          "1,null,null,", "2,1.0,null,", "3,null,2.0,", "4,2.0,null,", "5,1.0,2.0,", "6,null,1.0,"
        };
    resultSetEqualTest("select Diff(s1), diff(s2) from root.test", expectedHeader, retArray);
  }

  @Test
  public void testNewTransformerRespectNull() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "Diff(root.test.s1, \"ignoreNull\"=\"false\")",
          "diff(root.test.s2, \"ignoreNull\"=\"false\")"
        };
    String[] retArray =
        new String[] {
          "1,null,null,",
          "2,1.0,null,",
          "3,null,null,",
          "4,null,null,",
          "5,1.0,null,",
          "6,null,1.0,"
        };
    resultSetEqualTest(
        "select Diff(s1, 'ignoreNull'='false'), diff(s2, 'ignoreNull'='false') from root.test",
        expectedHeader,
        retArray);
  }

  // [change_points] is not mappable function, so this calculation use old transformer
  @Test
  public void testOldTransformerIgnoreNull() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, "change_points(root.test.s1)", "diff(root.test.s2)"};
    String[] retArray =
        new String[] {
          "1,1,null,", "2,2,null,", "3,null,2.0,", "4,4,null,", "5,5,2.0,", "6,null,1.0,"
        };
    resultSetEqualTest(
        "select change_points(s1), diff(s2) from root.test", expectedHeader, retArray);
  }

  @Test
  public void testOldTransformerRespectNull() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "change_points(root.test.s1)",
          "diff(root.test.s2, \"ignoreNull\"=\"false\")"
        };
    String[] retArray =
        new String[] {"1,1,null,", "2,2,null,", "4,4,null,", "5,5,null,", "6,null,1.0,"};
    resultSetEqualTest(
        "select change_points(s1), diff(s2, 'ignoreNull'='false') from root.test",
        expectedHeader,
        retArray);
  }
}
