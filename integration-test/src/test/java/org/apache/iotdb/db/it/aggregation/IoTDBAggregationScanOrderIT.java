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

import static org.apache.iotdb.db.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAggregationScanOrderIT {

  private static final String[] sqls =
      new String[] {
        "insert into root.sg1.d1(time, s1) values (12, 12);",
        "flush;",
        "insert into root.sg1.d1(time, s2) values (30, 30);",
        "flush;",
        "insert into root.sg1.d1(time, s1) values (0, 0);",
        "insert into root.sg1.d1(time, s1) values (8, 8);",
        "flush;",
        "insert into root.sg1.d1(time, s1) values (0, 0);",
        "insert into root.sg1.d1(time, s1) values (10, 10);",
        "flush;",
        "insert into root.sg1.d1(time, s1) values (17, 17);",
        "insert into root.sg1.d1(time, s1) values (20, 20);",
        "flush;",
        "insert into root.sg1.d2(time, s1) aligned values (12, 12);",
        "flush;",
        "insert into root.sg1.d2(time, s2) aligned values (30, 30);",
        "flush;",
        "insert into root.sg1.d2(time, s1) aligned values (0, 0);",
        "insert into root.sg1.d2(time, s1) aligned values (8, 8);",
        "flush;",
        "insert into root.sg1.d2(time, s1) aligned values (0, 0);",
        "insert into root.sg1.d2(time, s1) aligned values (10, 10);",
        "flush;",
        "insert into root.sg1.d2(time, s1) aligned values (17, 17);",
        "insert into root.sg1.d2(time, s1) aligned values (20, 20);",
        "flush;"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(sqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() {
    String d1s1 = "root.sg1.d1.s1";
    String[] expectedHeader = new String[] {firstValue(d1s1), lastValue(d1s1)};
    String[] retArray = new String[] {"0.0,20.0,"};

    resultSetEqualWithDescOrderTest(
        "select first_value(s1), last_value(s1) from root.sg1.d1", expectedHeader, retArray);
  }

  @Test
  public void alignedTest() {
    String d2s1 = "root.sg1.d2.s1";
    String[] expectedHeader = new String[] {firstValue(d2s1), lastValue(d2s1)};
    String[] retArray = new String[] {"0.0,20.0,"};

    resultSetEqualWithDescOrderTest(
        "select first_value(s1), last_value(s1) from root.sg1.d2", expectedHeader, retArray);
  }
}
