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
package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.db.it.utils.AlignedWriteUtil;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByQueryWithoutValueFilterWithDeletion2IT
    extends IoTDBGroupByQueryWithoutValueFilterWithDeletionIT {

  private static int numOfPointsPerPage;

  @BeforeClass
  public static void setUp() throws Exception {
    enableSeqSpaceCompaction = ConfigFactory.getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction = ConfigFactory.getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction = ConfigFactory.getConfig().isEnableCrossSpaceCompaction();
    numOfPointsPerPage = ConfigFactory.getConfig().getMaxNumberOfPointsInPage();
    maxTsBlockLineNumber = ConfigFactory.getConfig().getMaxTsBlockLineNumber();

    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(false);
    ConfigFactory.getConfig().setMaxNumberOfPointsInPage(3);
    ConfigFactory.getConfig().setMaxTsBlockLineNumber(3);
    EnvFactory.getEnv().initBeforeClass();
    AlignedWriteUtil.insertData();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete from root.sg1.d1.s1 where time <= 15");
      statement.execute("delete timeseries root.sg1.d1.s2");
      statement.execute("delete from root.sg1.d1.s3 where time > 25");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    ConfigFactory.getConfig().setMaxNumberOfPointsInPage(numOfPointsPerPage);
    ConfigFactory.getConfig().setMaxTsBlockLineNumber(maxTsBlockLineNumber);
  }
}
