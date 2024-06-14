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

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBVerifyConnectionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testVerifyConnectionAllUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("verify connection");
      Assert.assertEquals(
          ImmutableMap.of(ThriftService.STATUS_UP, 12), collectConnectionResult(resultSet));
      resultSet = statement.executeQuery("verify connection details");
      Assert.assertEquals(
          ImmutableMap.of(ThriftService.STATUS_UP, 54), collectConnectionResult(resultSet));
    }
  }

  @Test
  public void testVerifyConnectionWithNodeCrash() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 1);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("verify connection details");
      Assert.assertEquals(
          ImmutableMap.of(ThriftService.STATUS_UP, 18), collectConnectionResult(resultSet));
      final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
      int shutdownIndex = 0;
      if (shutdownIndex == leaderIndex) {
        shutdownIndex++;
      }
      EnvFactory.getEnv().getConfigNodeWrapperList().get(shutdownIndex++).stopForcibly();
      resultSet = statement.executeQuery("verify connection");
      Assert.assertEquals(
          ImmutableMap.of(ThriftService.STATUS_UP, 2, ThriftService.STATUS_DOWN, 7),
          collectConnectionResult(resultSet));
      resultSet = statement.executeQuery("verify connection details");
      Assert.assertEquals(
          ImmutableMap.of(ThriftService.STATUS_UP, 11, ThriftService.STATUS_DOWN, 7),
          collectConnectionResult(resultSet));
    }
  }

  private static Map<String, Integer> collectConnectionResult(ResultSet resultSet)
      throws Exception {
    Map<String, Integer> map = new HashMap<>();
    while (resultSet.next()) {
      String result = resultSet.getString(3).split(" ")[0];
      map.computeIfPresent(result, (key, value) -> value + 1);
      map.putIfAbsent(result, 1);
    }
    return map;
  }

  private static void showResult(ResultSet resultSet) throws Exception {
    while (resultSet.next()) {
      System.out.printf(
          "%s %s %s\n", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
    }
  }
}
