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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.TestOnlyPlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodesProcedure;
import org.apache.iotdb.confignode.procedure.impl.testonly.NeverFinishProcedure;
import org.apache.iotdb.confignode.procedure.store.ConfigProcedureStore;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.consensus.exception.ConsensusException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpgradeFromWALToConsensusLayerTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(UpgradeFromWALToConsensusLayerTest.class);
  ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

  private static final String DATA_DIR = "data_UpgradeFromWALToConsensusLayerTest";

  @Before
  public void setUp() throws Exception {
    conf.setConsensusDir(DATA_DIR + File.separator + conf.getConsensusDir());
    conf.setSystemDir(DATA_DIR + File.separator + conf.getSystemDir());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.recursivelyDeleteFolder(DATA_DIR);
    conf.setConsensusDir(conf.getConsensusDir().replace(DATA_DIR + File.separator, ""));
    conf.setSystemDir(conf.getSystemDir().replace(DATA_DIR + File.separator, ""));
  }

  /**
   * This test will fully start the ConfigManager, generating some files that cannot be cleaned up,
   * which will affect other tests. Therefore, this test is not enabled by default.
   */
  @Ignore
  @Test
  public void test() throws IOException, ConsensusException, InterruptedException {
    // start configManager for the first time
    ConfigManager configManager = new ConfigManager();
    conf.setConfigNodeId(0);
    conf.setInternalAddress("127.0.0.1");
    configManager.initConsensusManager();

    // write some raft logs to increase index, otherwise cannot take snapshot
    configManager.getConsensusManager().write(new TestOnlyPlan());
    configManager.getConsensusManager().write(new TestOnlyPlan());
    configManager.getConsensusManager().write(new TestOnlyPlan());

    ProcedureInfo procedureInfo = configManager.getProcedureManager().getStore().getProcedureInfo();
    ConfigProcedureStore.createOldProcWalDir();
    List<TDataNodeLocation> removedDataNodes =
        Arrays.asList(
            new TDataNodeLocation(
                10000,
                new TEndPoint("127.0.0.1", 6600),
                new TEndPoint("127.0.0.1", 7700),
                new TEndPoint("127.0.0.1", 8800),
                new TEndPoint("127.0.0.1", 9900),
                new TEndPoint("127.0.0.1", 11000)),
            new TDataNodeLocation(
                10001,
                new TEndPoint("127.0.0.1", 6601),
                new TEndPoint("127.0.0.1", 7701),
                new TEndPoint("127.0.0.1", 8801),
                new TEndPoint("127.0.0.1", 9901),
                new TEndPoint("127.0.0.1", 11001)));

    // prepare procedures
    Map<Integer, NodeStatus> nodeStatusMap = new HashMap<>();
    nodeStatusMap.put(10000, NodeStatus.Running);
    nodeStatusMap.put(10001, NodeStatus.Running);
    RemoveDataNodesProcedure removeDataNodesProcedure =
        new RemoveDataNodesProcedure(removedDataNodes, nodeStatusMap);
    removeDataNodesProcedure.setProcId(10086);
    AddConfigNodeProcedure addConfigNodeProcedure =
        new AddConfigNodeProcedure(
            new TConfigNodeLocation(
                0, new TEndPoint("0.0.0.0", 22277), new TEndPoint("0.0.0.0", 22278)),
            new TNodeVersionInfo());
    addConfigNodeProcedure.setProcId(888888);
    List<Procedure> procedureList =
        Arrays.asList(
            new NeverFinishProcedure(0),
            removeDataNodesProcedure,
            new NeverFinishProcedure(199),
            addConfigNodeProcedure,
            new NeverFinishProcedure(29999));
    procedureList.forEach(
        procedure -> procedureInfo.oldUpdateProcedure(new UpdateProcedurePlan(procedure)));

    // take snapshot manually
    procedureInfo.oldLoad();
    procedureInfo.upgrade();
    // check if wal files deleted
    Assert.assertFalse(procedureInfo.isOldVersion());

    // reactivate configManager to load snapshot
    configManager.close();
    configManager = new ConfigManager();
    configManager.initConsensusManager();
    // check procedures which loaded from snapshot
    List<Procedure<ConfigNodeProcedureEnv>> newProcedureList =
        configManager.getProcedureManager().getStore().getProcedureInfo().getProcedures();
    Assert.assertEquals(procedureList.size(), newProcedureList.size());
    Assert.assertTrue(newProcedureList.containsAll(procedureList));
  }
}
