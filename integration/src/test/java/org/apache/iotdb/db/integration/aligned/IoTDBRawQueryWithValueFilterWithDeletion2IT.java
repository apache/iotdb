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
package org.apache.iotdb.db.integration.aligned;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

@Category({LocalStandaloneTest.class})
public class IoTDBRawQueryWithValueFilterWithDeletion2IT
    extends IoTDBRawQueryWithValueFilterWithDeletionIT {

  private static int numOfPointsPerPage;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();

    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    numOfPointsPerPage = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(3);
    AlignedWriteUtil.insertData();
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("delete timeseries root.sg1.d1.s2");
      statement.execute("delete from root.sg1.d1.s1 where time <= 21");
      statement.execute("delete from root.sg1.d1.s5 where time <= 31 and time > 20");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(numOfPointsPerPage);
    EnvironmentUtils.cleanEnv();
  }
}
