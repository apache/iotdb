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
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBArithmeticDivAndModWithoutDoubleIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.test",
        "CREATE TIMESERIES root.test.sg1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.test.sg1(time,s1) values(1752034015183000006, 10001)",
        "flush",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimestampPrecision("ns");
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testArithmeticOperations() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "Time % 10", "Time / 10", "root.test.sg1.s1 % 10", "root.test.sg1.s1 / 10"
        };
    String[] retArray = new String[] {"1752034015183000006,6,175203401518300000,1,1000,"};
    resultSetEqualTest(
        "select time%10, time/10, s1%10, s1/10 from root.test.sg1", expectedHeader, retArray);
  }
}
