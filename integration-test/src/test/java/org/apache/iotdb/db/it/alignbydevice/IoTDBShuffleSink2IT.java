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

package org.apache.iotdb.db.it.alignbydevice;

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
public class IoTDBShuffleSink2IT {
  private static final String[] SINGLE_SERIES =
      new String[] {
        "create database root.single",
        "insert into root.single.d1(time,s1) values (1,1)",
        "insert into root.single.d1(time,s1) values (now(),2)",
        "insert into root.single.d2(time,s1) values (now(),3)",
        "insert into root.single.d2(time,s1) values (1,4)",
        "insert into root.single.d3(time,s1) values (now(),5)",
        "insert into root.single.d3(time,s1) values (1,6)"
      };

  // three devices, three data regions
  private static final String[] MULTI_SERIES =
      new String[] {
        "create database root.sg",
        "insert into root.sg.d1(time,s1,s2) values (1,1,1)",
        "insert into root.sg.d1(time,s1,s2) values (now(),2,2)",
        "insert into root.sg.d2(time,s1,s2) values (now(),3,3)",
        "insert into root.sg.d2(time,s1,s2) values (1,4,4)",
        "insert into root.sg.d3(time,s1,s2) values (now(),5,5)",
        "insert into root.sg.d3(time,s1,s2) values (1,6,6)"
      };

  // three devices, three data regions, d3 has only one region
  private static final String[] SECOND_MULTI_SERIES =
      new String[] {
        "create database root.sg1",
        "insert into root.sg1.d1(time,s1,s2) values (1,1,1)",
        "insert into root.sg1.d1(time,s1,s2) values (now(),2,2)",
        "insert into root.sg1.d2(time,s1,s2) values (now(),3,3)",
        "insert into root.sg1.d2(time,s1,s2) values (1,4,4)",
        "insert into root.sg1.d3(time,s1,s2) values (1,6,6)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDataRegionGroupExtensionPolicy("CUSTOM");
    EnvFactory.getEnv().getConfig().getCommonConfig().setDefaultDataRegionGroupNumPerDatabase(3);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SINGLE_SERIES);
    prepareData(MULTI_SERIES);
    prepareData(SECOND_MULTI_SERIES);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCountAlignByDeviceOrderByDeviceWithoutValueFilter() {
    // result of SINGLE_SERIES
    String expectedHeader1 = "Device,count(s1),";
    String[] retArray1 =
        new String[] {"root.single.d1,2,", "root.single.d2,2,", "root.single.d3,2,"};

    resultSetEqualTest(
        "select count(s1) from root.single.** align by device", expectedHeader1, retArray1);

    // result of MULTI_SERIES
    String expectedHeader2 = "Device,count(s1),count(s2),";
    String[] retArray2 = new String[] {"root.sg.d1,2,2,", "root.sg.d2,2,2,", "root.sg.d3,2,2,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.sg.** align by device", expectedHeader2, retArray2);

    // result of SECOND_MULTI_SERIES
    String expectedHeader3 = "Device,count(s1),count(s2),";
    String[] retArray3 = new String[] {"root.sg1.d1,2,2,", "root.sg1.d2,2,2,", "root.sg1.d3,1,1,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.sg1.** align by device", expectedHeader3, retArray3);
  }

  @Test
  public void testCountAlignByDeviceOrderByDeviceWithValueFilter() {
    // result of SINGLE_SERIES
    String expectedHeader1 = "Device,count(s1),";
    String[] retArray1 =
        new String[] {"root.single.d1,2,", "root.single.d2,2,", "root.single.d3,0,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.single.** where s1 <= 4 align by device",
        expectedHeader1,
        retArray1);

    // result of MULTI_SERIES
    String expectedHeader2 = "Device,count(s1),count(s2),";
    String[] retArray2 = new String[] {"root.sg.d1,2,2,", "root.sg.d2,2,2,", "root.sg.d3,0,0,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.sg.** where s1 <= 4 align by device",
        expectedHeader2,
        retArray2);

    // result of SECOND_MULTI_SERIES
    String expectedHeader3 = "Device,count(s1),count(s2),";
    String[] retArray3 = new String[] {"root.sg1.d1,2,2,", "root.sg1.d2,2,2,", "root.sg1.d3,0,0,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.sg1.** where s1 <= 4 align by device",
        expectedHeader3,
        retArray3);
  }

  @Test
  public void testCountAlignByDeviceOrderByTimeWithoutValueFilter() {
    // result of SINGLE_SERIES
    String expectedHeader1 = "Device,count(s1),";
    String[] retArray1 =
        new String[] {"root.single.d1,2,", "root.single.d2,2,", "root.single.d3,2,"};

    resultSetEqualTest(
        "select count(s1) from root.single.** order by time align by device",
        expectedHeader1,
        retArray1);

    // result of MULTI_SERIES
    String expectedHeader2 = "Device,count(s1),count(s2),";
    String[] retArray2 = new String[] {"root.sg.d1,2,2,", "root.sg.d2,2,2,", "root.sg.d3,2,2,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.sg.** order by time align by device",
        expectedHeader2,
        retArray2);

    // result of SECOND_MULTI_SERIES
    String expectedHeader3 = "Device,count(s1),count(s2),";
    String[] retArray3 = new String[] {"root.sg1.d1,2,2,", "root.sg1.d2,2,2,", "root.sg1.d3,1,1,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.sg1.** order by time align by device",
        expectedHeader3,
        retArray3);
  }

  @Test
  public void testCountAlignByDeviceOrderByTimeWithValueFilter() {
    // result of SINGLE_SERIES
    String expectedHeader1 = "Device,count(s1),";
    String[] retArray1 =
        new String[] {"root.single.d1,2,", "root.single.d2,2,", "root.single.d3,0,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.single.** where s1 <= 4 order by time align by device",
        expectedHeader1,
        retArray1);

    // result of MULTI_SERIES
    String expectedHeader2 = "Device,count(s1),count(s2),";
    String[] retArray2 = new String[] {"root.sg.d1,2,2,", "root.sg.d2,2,2,", "root.sg.d3,0,0,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.sg.** where s1 <= 4 order by time align by device",
        expectedHeader2,
        retArray2);

    // result of MULTI_SERIES
    String expectedHeader3 = "Device,count(s1),count(s2),";
    String[] retArray3 = new String[] {"root.sg1.d1,2,2,", "root.sg1.d2,2,2,", "root.sg1.d3,0,0,"};

    resultSetEqualTest(
        "select count(s1),count(s2) from root.sg1.** where s1 <= 4 order by time align by device",
        expectedHeader3,
        retArray3);
  }
}
