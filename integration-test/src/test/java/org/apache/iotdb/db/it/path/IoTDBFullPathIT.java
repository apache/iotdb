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

package org.apache.iotdb.db.it.path;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.count;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBFullPathIT {
  private static final String[] SQLs = new String[] {"INSERT INTO root.test(time,s1) VALUES(1,1)"};

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
  public void testUnsatisfiedRuleQuery() {
    assertTestFail(
        "select root.test.s1 from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Path can not start with root in select clause.");
  }

  @Test
  public void testSatisfiedRuleQuery() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "root.test.s1"};
    String[] retArray = new String[] {"1,1.0,"};
    resultSetEqualTest(
        "select s1 from root.** " + "where root.test.s1 is not null", expectedHeader, retArray);

    resultSetEqualTest(
        "select s1 from root.** " + "where root.test.s1 not in (2,3)", expectedHeader, retArray);

    expectedHeader = new String[] {count("root.test.s1")};
    retArray = new String[] {};
    resultSetEqualTest(
        "select count(s1) from root.** " + "having count(root.test.s1) not in (1,2)",
        expectedHeader,
        retArray);
  }
}
