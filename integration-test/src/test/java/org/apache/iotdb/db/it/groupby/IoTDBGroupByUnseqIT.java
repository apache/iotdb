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
package org.apache.iotdb.db.it.groupby;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByUnseqIT {

  private static final String[] dataSet1 =
      new String[] {
        "CREATE DATABASE root.sg1",
        "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(time,s1) values(1, 1)",
        "INSERT INTO root.sg1.d1(time,s1) values(2, 2)",
        "INSERT INTO root.sg1.d1(time,s1) values(3, 3)",
        "INSERT INTO root.sg1.d1(time,s1) values(4, 4)",
        "INSERT INTO root.sg1.d1(time,s1) values(8, 8)",
        "INSERT INTO root.sg1.d1(time,s1) values(10, 10)",
        "INSERT INTO root.sg1.d1(time,s1) values(11, 11)",
        "INSERT INTO root.sg1.d1(time,s1) values(12, 12)",
        "flush",
        "INSERT INTO root.sg1.d1(time,s1) values(7, 7)",
        "INSERT INTO root.sg1.d1(time,s1) values(9, 9)",
        "flush"
      };

  private static final String[] dataSet2 =
      new String[] {
        "CREATE DATABASE root.sg2",
        "CREATE TIMESERIES root.sg2.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg2.d1(time,s1) values(1, 1)",
        "INSERT INTO root.sg2.d1(time,s1) values(10, 10)",
        "flush",
        "INSERT INTO root.sg2.d1(time,s1) values(19, 19)",
        "INSERT INTO root.sg2.d1(time,s1) values(30, 30)",
        "flush",
        "INSERT INTO root.sg2.d1(time,s1) values(5, 5)",
        "INSERT INTO root.sg2.d1(time,s1) values(15, 15)",
        "INSERT INTO root.sg2.d1(time,s1) values(26, 26)",
        "INSERT INTO root.sg2.d1(time,s1) values(30, 30)",
        "flush"
      };

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * This test contains one seq file and one unseq file. In the seq file, it contains two pages:
   * 1,2,3,4 in one page, 8,10,11,12 in another page. In the unseq file, it only contains one page:
   * 7, 9. The unseq page is overlapped with the second seq page.
   */
  @Test
  public void test1() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxNumberOfPointsInPage(4);
    EnvFactory.getEnv().initClusterEnvironment();
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.sg1.d1.s1")};
    String[] retArray =
        new String[] {
          "1,3,", "4,1,", "7,3,", "10,3,",
        };

    prepareData(dataSet1);
    resultSetEqualTest(
        "select count(s1) from root.sg1.d1 group by ([1, 13), 3ms)", expectedHeader, retArray);
  }

  /**
   * This test contains two seq files and one unseq file. In the first seq file, it contains two
   * points: [1, 10]. In the second seq file, it contains two points: [19, 30]. In the unseq file,
   * it contains two CHUNKS: [5, 15], [26, 30]. The unseq file is overlapped with two seq files.
   * While the chunk [19,30] in the second seq file is unpacked, it should replace [26,30] as the
   * first chunk.
   */
  @Test
  public void test2() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setMaxNumberOfPointsInPage(4)
        .setAvgSeriesPointNumberThreshold(2);
    EnvFactory.getEnv().initClusterEnvironment();
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.sg2.d1.s1")};
    String[] retArray = new String[] {"5,1,", "10,1,", "15,2,", "20,0,", "25,1,"};

    prepareData(dataSet2);
    resultSetEqualTest(
        "select count(s1) from root.sg2.d1 group by ([5, 30), 5ms)", expectedHeader, retArray);
  }
}
