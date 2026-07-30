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

package org.apache.iotdb.db.it.udf;

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
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDFIntermediateBlockSerdeIT {
  private static final String[] SQLs =
      new String[] {
        "insert into root.sg.d1(time, s1) values (1,1)",
        "insert into root.sg.d1(time, s1) values (2,2)",
        "insert into root.sg.d1(time, s1) values (3,3)",
        "insert into root.sg.d1(time, s1) values (4,4)",
        "insert into root.sg.d1(time, s1) values (5,5)",
        "insert into root.sg.d1(time, s1) values (6,6)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setUdfMemoryBudgetInMB(0.0001f);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testM4() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "EQUAL_SIZE_BUCKET_M4_SAMPLE(root.sg.d1.s1, \"proportion\"=\"1\")"
        };
    String[] retArray = new String[] {"1,1.0,", "2,2.0,", "3,3.0,", "4,4.0,", "5,5.0,", "6,6.0,"};
    resultSetEqualWithDescOrderTest(
        "select EQUAL_SIZE_BUCKET_M4_SAMPLE(s1,'proportion'='1') from root.sg.d1",
        expectedHeader,
        retArray);
  }
}
