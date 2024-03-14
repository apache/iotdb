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

package org.apache.iotdb.db.it.alignbydevice;

import org.apache.iotdb.it.env.EnvFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.insertData;
import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.insertData2;

public class IoTDBOrderByLimitOffsetAlignByDevice2IT
    extends IoTDBOrderByLimitOffsetAlignByDeviceIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSeriesSlotNum(1);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
    insertData2();
    insertData3();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }
}
