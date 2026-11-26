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

package org.apache.iotdb.pipe.it.dual.treemodel.manual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeManual;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipeMetaRestartIT extends AbstractPipeTreeModelManualSharedClusterIT {

  private static final String SCHEMA_DATABASE = "root.meta_restart_schema";
  private static final String CONFIG_DATABASE_PREFIX = "root.meta_restart_config";

  // Create ten timeseries before restarting both clusters and ten afterward; the recovered schema
  // task must replicate all twenty into this scenario's database.
  @Test
  public void testAutoRestartSchemaTask() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    createTimeseries(0, 10);
    restartClusters();
    createTimeseries(10, 20);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeseries " + SCHEMA_DATABASE + ".**",
        "count(timeseries),",
        Collections.singleton("20,"));
  }

  // Create ten databases before restarting both clusters and ten afterward; the recovered config
  // task must replicate all twenty, using a prefix distinct from the schema-restart scenario.
  @Test
  public void testAutoRestartConfigTask() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    createDatabases(0, 10);
    restartClusters();
    createDatabases(10, 20);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count databases " + CONFIG_DATABASE_PREFIX + "*",
        "count,",
        Collections.singleton("20,"));
  }

  private void createTimeseries(final int startInclusive, final int endExclusive) throws Exception {
    // Reuse one connection per phase, closing it before the cluster restart.
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = startInclusive; i < endExclusive; ++i) {
        session.executeNonQueryStatement(
            String.format(
                "create timeseries %s.wf01.GPS.status%d with datatype=BOOLEAN,encoding=PLAIN",
                SCHEMA_DATABASE, i));
      }
    }
  }

  private void createDatabases(final int startInclusive, final int endExclusive) throws Exception {
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = startInclusive; i < endExclusive; ++i) {
        session.executeNonQueryStatement("create database " + CONFIG_DATABASE_PREFIX + i);
      }
    }
  }

  private void restartClusters() {
    // Keep the receiver available while the sender restores its pipe tasks: restarting both at
    // once can fail the recovery handshake and wait for the next periodic metadata sync.
    // Propagate restart failures so an unsuccessful recovery cannot pass without the assertions.
    TestUtils.restartCluster(senderEnv);
    TestUtils.restartCluster(receiverEnv);
  }
}
