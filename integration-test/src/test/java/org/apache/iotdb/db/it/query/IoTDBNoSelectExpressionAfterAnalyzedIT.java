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

package org.apache.iotdb.db.it.query;

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
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.count;
import static org.apache.iotdb.itbase.constant.TestConstant.s1;
import static org.apache.iotdb.itbase.constant.TestConstant.s2;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBNoSelectExpressionAfterAnalyzedIT {
  private static final String[] SQLs =
      new String[] {
        "insert into root.sg.d1(time,s1) values(1,1)",
        "insert into root.sg.d2(time,s1,s2) values(1,1,1)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAlignByDevice() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR};
    String[] retArray = new String[] {};
    resultSetEqualTest(
        "select s2 from root.sg.d1 where s1>0 align by device", expectedHeader, retArray);

    resultSetEqualTest(
        "select count(s2) from root.sg.d1 where s1>0 align by device", expectedHeader, retArray);

    // mix test
    expectedHeader = new String[] {DEVICE, count(s1), count(s2)};
    retArray = new String[] {"root.sg.d1,1,null,", "root.sg.d2,1,1,"};
    resultSetEqualTest(
        "select count(s1), count(s2) from root.sg.* where s1>0 align by device",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {TIMESTAMP_STR, DEVICE, s1, s2};
    retArray = new String[] {"1,root.sg.d1,1.0,null,", "1,root.sg.d2,1.0,1.0,"};
    resultSetEqualTest(
        "select s1, s2 from root.sg.* where s1>0 align by device", expectedHeader, retArray);
  }

  @Test
  public void testAlignByTime() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR};
    String[] retArray = new String[] {};
    resultSetEqualTest("select s2 from root.sg.d1 where s1>0", expectedHeader, retArray);

    resultSetEqualTest("select count(s2) from root.sg.d1 where s1>0", expectedHeader, retArray);
  }
}
