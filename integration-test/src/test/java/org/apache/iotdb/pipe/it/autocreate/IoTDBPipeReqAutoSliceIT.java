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

package org.apache.iotdb.pipe.it.autocreate;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeReqAutoSliceIT extends AbstractPipeDualAutoIT {

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv.getConfig().getCommonConfig().setPipeConnectorRequestSliceThresholdBytes(4);
    receiverEnv.getConfig().getCommonConfig().setPipeConnectorRequestSliceThresholdBytes(4);
  }

  @Test
  public void pipeReqAutoSliceTest() {
    try {
      createTimeSeries();
      ISession senderSession = senderEnv.getSessionConnection();
      createPipe(senderSession);
      Thread.sleep(2000);
      List<Pair<Long, Integer>> data = createTestDataForInt32();
      executeDataInsertions(senderSession, data);
      verify(data);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private void createPipe(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        String.format(
            "create pipe test"
                + " with source ('source'='iotdb-source','source.path'='root.test.**')"
                + " with sink ('sink'='iotdb-thrift-sync-sink','node-urls'='%s:%s','batch.enable'='false')",
            receiverEnv.getIP(), receiverEnv.getPort()));
  }

  private void executeDataInsertions(ISession session, List<Pair<Long, Integer>> data)
      throws IoTDBConnectionException, StatementExecutionException {
    for (Pair<Long, Integer> pairs : data) {
      session.executeNonQueryStatement(
          String.format(
              "insert into root.test.db (time,status) values (%d,%d)", pairs.left, pairs.right));
    }
    session.executeNonQueryStatement("flush");
  }

  private List<Pair<Long, Integer>> createTestDataForInt32() {
    List<Pair<Long, Integer>> pairs = new ArrayList<>();
    Random random = new Random();
    for (long i = 0; i < 100; i++) {
      pairs.add(new Pair<>(i, random.nextInt()));
    }
    return pairs;
  }

  private void verify(List<Pair<Long, Integer>> data) {
    HashSet<String> set = new HashSet<>();
    for (Pair<Long, Integer> pair : data) {
      set.add(String.format("%d,%d,", pair.left, pair.right));
    }
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.test.**", "Time,root.test.db.status,", set);
  }

  private void createTimeSeries() {
    String timeSeriesCreation =
        String.format("create timeseries root.test.db.status with datatype=INT32,encoding=PLAIN");
    TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv, Collections.singletonList(timeSeriesCreation));
    TestUtils.tryExecuteNonQueriesWithRetry(
        receiverEnv, Collections.singletonList(timeSeriesCreation));
  }
}
