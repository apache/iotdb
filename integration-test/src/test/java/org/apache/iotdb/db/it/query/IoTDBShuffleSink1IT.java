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

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShuffleSink1IT {

  // two devices
  private static final String[] SQLs =
      new String[] {
        "insert into root.sg.d1(time,s1,s2) values (1,2,2)",
        "insert into root.sg.d1(time,s1,s2) values (now(),3,3)",
        "insert into root.sg.d2(time,s1,s2) values (1,4,4)",
        "insert into root.sg.d2(time,s1,s2) values (now(),5,5)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDataRegionGroupExtensionPolicy("CUSTOM");
    EnvFactory.getEnv().getConfig().getCommonConfig().setDefaultDataRegionGroupNumPerDatabase(2);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCountAlignByDeviceOrderByDeviceWithoutValueFilter() {
    String expectedHeader = "Device,count(s1),count(s2)";
    String[] retArray = new String[] {"root.sg.d1,2,2", "root.sg.d2,2,2"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.** align by device", expectedHeader, retArray);
  }

  @Test
  public void testCountAlignByDeviceOrderByDeviceWithValueFilter() {
    String expectedHeader = "Device,count(s1),count(s2)";
    String[] retArray = new String[] {"root.sg.d1,2,2", "root.sg.d2,1,1"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.** where s1 <= 4 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testCountAlignByDeviceOrderByTimeWithoutValueFilter() {
    String expectedHeader = "Device,count(s1),count(s2)";
    String[] retArray = new String[] {"root.sg.d1,2,2", "root.sg.d2,2,2"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.** order by time align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testCountAlignByDeviceOrderByTimeWithValueFilter() {
    String expectedHeader = "Device,count(s1),count(s2)";
    String[] retArray = new String[] {"root.sg.d1,2,2", "root.sg.d2,1,1"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.** where s1 <= 4 order by time align by device",
        expectedHeader,
        retArray);
  }
}
