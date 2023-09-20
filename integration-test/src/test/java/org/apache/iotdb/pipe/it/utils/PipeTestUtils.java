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

package org.apache.iotdb.pipe.it.utils;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.itbase.env.BaseEnv;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

public class PipeTestUtils {
  public static void restartCluster(BaseEnv env) throws Exception {
    for (int i = 0; i < env.getConfigNodeWrapperList().size(); ++i) {
      env.shutdownConfigNode(i);
    }
    for (int i = 0; i < env.getDataNodeWrapperList().size(); ++i) {
      env.shutdownDataNode(i);
    }
    TimeUnit.SECONDS.sleep(1);
    for (int i = 0; i < env.getConfigNodeWrapperList().size(); ++i) {
      env.startConfigNode(i);
    }
    for (int i = 0; i < env.getDataNodeWrapperList().size(); ++i) {
      env.startDataNode(i);
    }
    ((AbstractEnv) env).testWorkingNoUnknown();
  }

  public static void assertDataOnEnv(
      BaseEnv env, String sql, String expectedHeader, Set<String> expectedResSet) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      await()
          .atMost(600, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  TestUtils.assertResultSetEqual(
                      statement.executeQuery(sql), expectedHeader, expectedResSet));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
