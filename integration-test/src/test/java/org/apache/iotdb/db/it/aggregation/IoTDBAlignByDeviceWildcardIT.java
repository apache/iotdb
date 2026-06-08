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
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBAlignByDeviceWildcardIT {

  private static final String[] SQL_LIST =
      new String[] {
        "CREATE DATABASE root.min",
        "CREATE TIMESERIES root.min.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.min.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.min.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.min.d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.min.d1(time, s1, s2) VALUES(1, 1, 1)",
        "INSERT INTO root.min.d1(time, s1, s2) VALUES(2, 1, 1)",
        "FLUSH",
        "INSERT INTO root.min.d2(time, s1, s2) VALUES(5, 1, 1)",
        "FLUSH"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setTimePartitionInterval(1)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQL_LIST);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testWildcardAlignByDeviceWithTimePartitionSplit() {
    String sql =
        "SELECT count(s1) FROM root.min.** "
            + "WHERE s2 is not null and s1 is not null "
            + "GROUP BY([1, 6), 1ms) ALIGN BY DEVICE";
    String[] expectedHeader = new String[] {"Time", "Device", "count(s1)"};
    String[] expectedRows =
        new String[] {
          "1,root.min.d1,1,",
          "2,root.min.d1,1,",
          "3,root.min.d1,0,",
          "4,root.min.d1,0,",
          "5,root.min.d1,0,",
          "1,root.min.d2,0,",
          "2,root.min.d2,0,",
          "3,root.min.d2,0,",
          "4,root.min.d2,0,",
          "5,root.min.d2,1,",
        };
    resultSetEqualTest(sql, expectedHeader, expectedRows);
  }
}
