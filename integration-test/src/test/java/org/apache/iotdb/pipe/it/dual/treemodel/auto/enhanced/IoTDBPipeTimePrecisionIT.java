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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.pipe.it.dual.PipeTransferObserver;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.assertInt32Schema;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.assertRows;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.awaitDrained;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.awaitRows;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.createPipe;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.execute;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.showPipe;
import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeTimePrecisionIT extends AbstractPipeDualTreeModelAutoIT {
  @Rule public final TestName testName = new TestName();

  @Override
  protected void setupConfig() {
    super.setupConfig();
    // These cases require different immutable cluster precision settings and cannot share nodes.
    final boolean highPrecision = testName.getMethodName().contains("Nanosecond");
    senderEnv.getConfig().getCommonConfig().setTimestampPrecision(highPrecision ? "ns" : "ms");
    receiverEnv.getConfig().getCommonConfig().setTimestampPrecision(highPrecision ? "ns" : "us");
  }

  @Test
  public void testTimestampPrecisionMismatchIsReportedAndDoesNotCreateSchema() throws Exception {
    // A6a: a reachable receiver rejects an incompatible handshake before any schema/data write.
    final String name = "precision_mismatch";
    final String database = "root.pipe_precision_mismatch";
    // Allocate the source region before CREATE/START, so their procedure records the rejected
    // handshake. Creating the region later exercises separate background metadata discovery.
    execute(
        senderEnv,
        "create timeseries " + database + ".d1.s1 INT32 encoding=RLE",
        "insert into " + database + ".d1(time,s1) values(1,10)");
    try (final PipeTransferObserver observer =
        new PipeTransferObserver(receiverEnv.getDataNodeWrapper(0))) {
      createPipe(senderEnv, name, database, observer.getEndpoint(), Map.of(), Map.of(), Map.of());
      try {
        await()
            .atMost(60, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  Assert.assertTrue(
                      "No explicit precision rejection arrived from the receiver",
                      observer.getTransfers().stream()
                          .anyMatch(
                              t ->
                                  t.request.getType()
                                          == PipeRequestType.HANDSHAKE_DATANODE_V2.getType()
                                      && t.response.getStatus().getCode()
                                          == TSStatusCode.PIPE_HANDSHAKE_ERROR.getStatusCode()
                                      && t.response.getStatus().getMessage().contains("us")
                                      && t.response.getStatus().getMessage().contains("ms")));
                  final TShowPipeInfo info = showPipe(senderEnv, name);
                  Assert.assertEquals("STOPPED", info.getState());
                  final String exception = info.getExceptionMessage();
                  Assert.assertNotNull(exception);
                  Assert.assertTrue(
                      exception,
                      exception.contains(
                          Integer.toString(TSStatusCode.PIPE_HANDSHAKE_ERROR.getStatusCode())));
                });
        assertRows(receiverEnv, "count timeseries " + database + ".**", "count(timeseries),", "0,");
        Assert.assertFalse(
            "An incompatible connection must not send business records",
            observer.getTransfers().stream()
                .anyMatch(
                    t ->
                        t.request.getType() != PipeRequestType.HANDSHAKE_DATANODE_V1.getType()
                            && t.request.getType()
                                != PipeRequestType.HANDSHAKE_DATANODE_V2.getType()));
      } finally {
        execute(senderEnv, "drop pipe if exists " + name);
      }
      observer.awaitNoConnections();
    }
    await()
        .pollInSameThread()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (final Connection connection = receiverEnv.getConnection();
                  final Statement statement = connection.createStatement();
                  final ResultSet rows = statement.executeQuery("show receivers")) {
                Assert.assertFalse("Rejected handshake left a receiver runtime", rows.next());
              }
            });
  }

  @Test
  public void testNanosecondTimestampsOneUnitApartArePreserved() throws Exception {
    // A6b: adjacent ns timestamps and inclusive filtering boundaries must survive unchanged.
    final String database = "root.pipe_nanosecond_precision";
    final String device = database + ".d1";
    final String name = "nanosecond_precision";
    execute(senderEnv, "create timeseries " + device + ".s1 INT32 encoding=RLE");
    createPipe(
        senderEnv,
        name,
        database,
        receiverEnv.getDataNodeWrapper(0).getIpAndPortString(),
        Map.of("source.start-time", "1000000000001", "source.end-time", "1000000000002"),
        Map.of(),
        Map.of());
    try {
      execute(
          senderEnv,
          "insert into "
              + device
              + "(time,s1) values"
              + "(1000000000000,0),(1000000000001,11),(1000000000002,22),(1000000000003,33)");
      assertRows(
          senderEnv,
          "select * from " + device,
          "Time," + device + ".s1,",
          "1000000000000,0,",
          "1000000000001,11,",
          "1000000000002,22,",
          "1000000000003,33,");
      awaitRows(
          receiverEnv,
          "select * from " + device,
          "Time," + device + ".s1,",
          "1000000000001,11,",
          "1000000000002,22,");
      awaitDrained(senderEnv, name);
      assertInt32Schema(receiverEnv, device);
    } finally {
      execute(senderEnv, "drop pipe if exists " + name);
    }
  }
}
