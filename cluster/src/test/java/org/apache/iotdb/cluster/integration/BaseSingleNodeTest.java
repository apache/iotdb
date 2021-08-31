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

package org.apache.iotdb.cluster.integration;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.cluster.utils.Constants;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;

import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

public abstract class BaseSingleNodeTest {

  private MetaClusterServer metaServer;

  private boolean useAsyncServer;
  private List<String> seedNodeUrls;
  private int replicaNum;
  private boolean autoCreateSchema;

  @Before
  public void setUp() throws Exception {
    initConfigs();
    metaServer = new MetaClusterServer();
    metaServer.start();
    metaServer.buildCluster();
  }

  @After
  public void tearDown() throws Exception {
    metaServer.stop();
    recoverConfigs();
    EnvironmentUtils.cleanEnv();
  }

  private void initConfigs() {
    useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    seedNodeUrls = ClusterDescriptor.getInstance().getConfig().getSeedNodeUrls();
    replicaNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    autoCreateSchema = ClusterDescriptor.getInstance().getConfig().isEnableAutoCreateSchema();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    ClusterDescriptor.getInstance()
        .getConfig()
        .setSeedNodeUrls(
            Collections.singletonList(
                String.format("127.0.0.1:9003:40011:%d", Constants.RPC_PORT)));
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(1);
    ClusterDescriptor.getInstance().getConfig().setEnableAutoCreateSchema(true);
  }

  private void recoverConfigs() {
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    ClusterDescriptor.getInstance().getConfig().setSeedNodeUrls(seedNodeUrls);
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(replicaNum);
    ClusterDescriptor.getInstance().getConfig().setEnableAutoCreateSchema(autoCreateSchema);
  }

  public Session openSession() throws IoTDBConnectionException {
    Session session = new Session("127.0.0.1", Constants.RPC_PORT);
    session.open();
    return session;
  }
}
