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

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBPaginationIT {

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.vehicle",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
        "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
        "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
        "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
        "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
        "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
        "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
        "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
        "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
        "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
        "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
        "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)"
      };

  @BeforeClass
  public static void setUp() throws InterruptedException {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void rawDataQueryTest() {
    List<String> querySQLs =
        Arrays.asList(
            "SELECT s1 FROM root.vehicle.d0 WHERE time<200 limit 3",
            "SELECT s0 FROM root.vehicle.d0 WHERE s1 > 190 limit 3",
            "SELECT s1,s2 FROM root.vehicle.d0 WHERE s1 > 190 or s2 < 10.0 limit 3 offset 2",
            "SELECT * FROM root.vehicle.d0 slimit 1",
            "SELECT * FROM root.vehicle.d0 slimit 1 soffset 2",
            "SELECT * FROM root.vehicle.d0 WHERE s1 > 190 or s2 < 10.0 limit 3 offset 1 slimit 1 soffset 2");
    List<String> expectHeaders =
        Arrays.asList(
            "Time,root.vehicle.d0.s1,",
            "Time,root.vehicle.d0.s0,",
            "Time,root.vehicle.d0.s1,root.vehicle.d0.s2,",
            "Time,root.vehicle.d0.s0,",
            "Time,root.vehicle.d0.s2,",
            "Time,root.vehicle.d0.s2,");
    List<String[]> retArrays =
        Arrays.asList(
            new String[] {"1,1101,", "2,40000,", "50,50000,"},
            new String[] {"1,101,", "2,10000,", "50,10000,"},
            new String[] {"3,null,3.33,", "4,null,4.44,", "50,50000,null,"},
            new String[] {
              "1,101,",
              "2,10000,",
              "50,10000,",
              "100,99,",
              "101,99,",
              "102,80,",
              "103,99,",
              "104,90,",
              "105,99,",
              "106,99,",
              "1000,22222,"
            },
            new String[] {
              "2,2.22,", "3,3.33,", "4,4.44,", "102,10.0,", "105,11.11,", "1000,1000.11,"
            },
            new String[] {"3,3.33,", "4,4.44,", "105,11.11,"});

    for (int i = 0; i < querySQLs.size(); i++) {
      resultSetEqualTest(querySQLs.get(0), expectHeaders.get(0), retArrays.get(0));
    }
  }
}
