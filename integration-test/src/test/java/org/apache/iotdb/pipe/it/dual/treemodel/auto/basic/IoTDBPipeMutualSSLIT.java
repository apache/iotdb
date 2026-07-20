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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeMutualSSLIT extends AbstractPipeDualTreeModelAutoIT {

  private static final String STORE_PASSWORD = "thrift";

  @Override
  protected void setupConfig() {
    super.setupConfig();

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setTrustStorePath(trustStorePath())
        .setTrustStorePwd(STORE_PASSWORD)
        .setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setEnableThriftClientSSL(true)
        .setThriftSSLClientAuth(true)
        .setKeyStorePath(keyStorePath())
        .setKeyStorePwd(STORE_PASSWORD)
        .setTrustStorePath(trustStorePath())
        .setTrustStorePwd(STORE_PASSWORD)
        .setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
  }

  @Test
  public void testPipeCanTransferWithMutualSSL() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.pipe_mtls.d1(time, s1) values (1, 11)",
              "insert into root.pipe_mtls.d1(time, s1) values (2, 22)",
              "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.realtime.mode", "log");
      sourceAttributes.put("source.user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-ssl-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverDataNode.getIp());
      sinkAttributes.put("sink.port", Integer.toString(receiverDataNode.getPort()));
      sinkAttributes.put("sink.ssl.trust-store-path", trustStorePath());
      sinkAttributes.put("sink.ssl.trust-store-pwd", STORE_PASSWORD);
      sinkAttributes.put("sink.ssl.key-store-path", keyStorePath());
      sinkAttributes.put("sink.ssl.key-store-pwd", STORE_PASSWORD);

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.pipe_mtls.d1(time, s1) values (3, 33)", "flush"),
          null);

      try (final Connection connection =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX
                      + receiverDataNode.getIpAndPortString()
                      + "?"
                      + Config.USE_SSL
                      + "=true&"
                      + Config.TRUST_STORE
                      + "="
                      + trustStorePath()
                      + "&"
                      + Config.TRUST_STORE_PWD
                      + "="
                      + STORE_PASSWORD
                      + "&"
                      + Config.KEY_STORE
                      + "="
                      + keyStorePath()
                      + "&"
                      + Config.KEY_STORE_PWD
                      + "="
                      + STORE_PASSWORD,
                  SessionConfig.DEFAULT_USER,
                  SessionConfig.DEFAULT_PASSWORD);
          final Statement statement = connection.createStatement()) {
        Awaitility.await()
            .pollInSameThread()
            .pollDelay(1L, TimeUnit.SECONDS)
            .pollInterval(1L, TimeUnit.SECONDS)
            .atMost(600, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    TestUtils.assertResultSetEqual(
                        statement.executeQuery("select s1 from root.pipe_mtls.d1"),
                        "Time,root.pipe_mtls.d1.s1,",
                        Collections.unmodifiableSet(
                            new HashSet<>(Arrays.asList("1,11.0,", "2,22.0,", "3,33.0,")))));
      }
    }
  }

  private static String keyStorePath() {
    return keyDir() + "test-keystore";
  }

  private static String trustStorePath() {
    return keyDir() + "test-truststore";
  }

  private static String keyDir() {
    return System.getProperty("user.dir")
        + File.separator
        + "target"
        + File.separator
        + "test-classes"
        + File.separator;
  }
}
