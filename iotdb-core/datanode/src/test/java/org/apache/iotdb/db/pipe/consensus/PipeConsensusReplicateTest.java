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

package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskConnectorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.PipeConsensusConfig.PipeConsensusRPCConfig;
import org.apache.iotdb.consensus.pipe.service.PipeConsensusRPCService;
import org.apache.iotdb.consensus.pipe.service.PipeConsensusRPCServiceProcessor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletRawReq;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({
  "com.sun.org.apache.xerces.*",
  "javax.xml.*",
  "org.xml.*",
  "org.w3c.*",
  "javax.management.*"
})
@PrepareForTest(PipeConsensusTabletRawReq.class)
public class PipeConsensusReplicateTest {

  private final Logger logger = LoggerFactory.getLogger(PipeConsensusReplicateTest.class);

  // ============================= Consensus Components =============================

  private final ConsensusGroupId gid = new DataRegionId(1);
  private final DataRegionId did = new DataRegionId(1);
  private int basePort = 9000;
  // although a consensus group usually contains odd nums replicates, we set 2 replicates here for
  // test easily
  private final List<Peer> peers =
      Arrays.asList(
          // leader
          new Peer(gid, 1, new TEndPoint("127.0.0.1", basePort - 1)),
          // follower
          new Peer(gid, 2, new TEndPoint("127.0.0.1", basePort)));

  // ============================= RPC Mock Components =============================

  private static final long timeout = TimeUnit.SECONDS.toMillis(300);
  private final RegisterManager registerManager = new RegisterManager();

  // ============================= Pipe Components =============================

  private final PipeConsensusAsyncConnector leaderSink = new PipeConsensusAsyncConnector();

  // ============================= Data Generate =============================

  private final List<EnrichedEvent> mockWALEvents = new ArrayList<>();
  private final List<EnrichedEvent> mockTsFileEvents = new ArrayList<>();
  private final List<EnrichedEvent> mockHybridEvents = new ArrayList<>();
  private final String[] device = new String[] {"root", "sg", "d"};
  private final String committerKey = "pipeConsensusReplicate";
  private final int initialRebootTimes = 1;
  private final int walEventSize = 5;
  private final int tsFileEventsSize = 5;
  private File tsFileDir;

  @Before
  public void setUp() throws Exception {
    // prepare TsFile env
    File tmpDir = new File(Files.createTempDirectory(committerKey).toString());
    tsFileDir =
        new File(
            tmpDir.getPath()
                + File.separator
                + IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + "root.sg"
                + File.separator
                + "0");
    boolean ignoredRes = tsFileDir.mkdirs();

    // initialize port available
    for (int i = 0; i < peers.size(); i++) {
      findPortAvailable(i);
    }

    // initialize leader's transfer component
    PipeParameters leaderSinkParameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(
                    PipeConnectorConstant.CONNECTOR_KEY,
                    BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_CONNECTOR.getPipePluginName());
                put(
                    PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY,
                    peers.get(1).getEndpoint().getIp());
                put(
                    PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY,
                    String.valueOf(peers.get(1).getEndpoint().getPort()));
              }
            });
    leaderSink.validate(new PipeParameterValidator(leaderSinkParameters));
    String pipeName = "pipe_consensus_replicate_test";
    leaderSink.customize(
        leaderSinkParameters,
        new PipeTaskRuntimeConfiguration(
            new PipeTaskConnectorRuntimeEnvironment(
                pipeName, System.currentTimeMillis(), did.getId())));

    // initialize follower's receive component
    PipeConsensusRPCService followerRPCServer =
        new PipeConsensusRPCService(peers.get(1).getEndpoint(), new PipeConsensusRPCConfig());
    followerRPCServer.initAsyncedServiceImpl(
        new PipeConsensusRPCServiceProcessor(PipeAgent.receiver().pipeConsensus()));
    registerManager.register(followerRPCServer);

    // initialize WAL and tsFile mock events
    prepareWALEvents(walEventSize, initialRebootTimes);
    prepareTsFileEvents(0, initialRebootTimes, tsFileEventsSize);

    // mock static methods
    PowerMockito.mockStatic(PipeConsensusTabletRawReq.class);
    PowerMockito.when(
            PipeConsensusTabletRawReq.toTPipeConsensusTransferReq(
                Mockito.any(), Mockito.anyBoolean(), Mockito.any()))
        .thenAnswer(
            invocation -> {
              PipeConsensusTabletRawReq pipeConsensusRawReq = new PipeConsensusTabletRawReq();
              pipeConsensusRawReq.commitId = invocation.getArgument(2);
              pipeConsensusRawReq.type =
                  PipeConsensusRequestType.TRANSFER_TABLET_INSERT_NODE.getType();
              pipeConsensusRawReq.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
              // mock body
              try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
                  final DataOutputStream outputStream =
                      new DataOutputStream(byteArrayOutputStream)) {
                ReadWriteIOUtils.write(false, outputStream);
                ReadWriteIOUtils.write(123, outputStream);
                pipeConsensusRawReq.body =
                    ByteBuffer.wrap(
                        byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
              }
              return pipeConsensusRawReq;
            });
  }

  @After
  public void tearDown() throws Exception {
    leaderSink.close();
    registerManager.deregisterAll();
  }

  @Test
  public void sequenceReplicateWALTest() throws Exception {
    PipeAgent.receiver().pipeConsensus().resetReceiver();

    for (int i = 0; i < mockWALEvents.size(); i++) {
      leaderSink.transfer((TabletInsertionEvent) mockWALEvents.get(i));
      Assert.assertEquals(1, leaderSink.getTransferBufferSize());
      Thread.sleep(50);

      Assert.assertEquals(i, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
      Assert.assertEquals(0, leaderSink.getRetryBufferSize());
      Assert.assertEquals(0, leaderSink.getTransferBufferSize());
    }
  }

  @Test
  public void unsequenceReplicateWALTest() throws Exception {
    PipeAgent.receiver().pipeConsensus().resetReceiver();

    // send 3
    leaderSink.transfer((TabletInsertionEvent) mockWALEvents.get(3));
    Thread.sleep(50);
    Assert.assertEquals(-1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(1, leaderSink.getTransferBufferSize());
    // send 4
    leaderSink.transfer((TabletInsertionEvent) mockWALEvents.get(4));
    Thread.sleep(50);
    Assert.assertEquals(-1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 0
    leaderSink.transfer((TabletInsertionEvent) mockWALEvents.get(0));
    Assert.assertEquals(3, leaderSink.getTransferBufferSize());
    Thread.sleep(50);
    Assert.assertEquals(0, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 1
    leaderSink.transfer((TabletInsertionEvent) mockWALEvents.get(1));
    Assert.assertEquals(3, leaderSink.getTransferBufferSize());
    Thread.sleep(50);
    Assert.assertEquals(1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 2
    leaderSink.transfer((TabletInsertionEvent) mockWALEvents.get(2));
    Thread.sleep(50);
    Assert.assertEquals(2, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    // wait until receiver process all events
    Thread.sleep(5000);
    Assert.assertEquals(4, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(0, leaderSink.getTransferBufferSize());
  }

  @Test
  public void sequenceReplicateTsFileTest() throws Exception {
    PipeAgent.receiver().pipeConsensus().resetReceiver();

    for (int i = 0; i < mockTsFileEvents.size(); i++) {
      leaderSink.transfer((TsFileInsertionEvent) mockTsFileEvents.get(i));
      Assert.assertEquals(1, leaderSink.getTransferBufferSize());
      Thread.sleep(200);

      Assert.assertEquals(i, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
      Assert.assertEquals(0, leaderSink.getRetryBufferSize());
      Assert.assertEquals(0, leaderSink.getTransferBufferSize());
    }
  }

  @Test
  public void unsequenceReplicateTsFileTest() throws Exception {
    PipeAgent.receiver().pipeConsensus().resetReceiver();

    // send 3
    leaderSink.transfer((TsFileInsertionEvent) mockTsFileEvents.get(3));
    Thread.sleep(50);
    Assert.assertEquals(-1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(1, leaderSink.getTransferBufferSize());
    // send 4
    leaderSink.transfer((TsFileInsertionEvent) mockTsFileEvents.get(4));
    Thread.sleep(50);
    Assert.assertEquals(-1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 0
    leaderSink.transfer((TsFileInsertionEvent) mockTsFileEvents.get(0));
    Assert.assertEquals(3, leaderSink.getTransferBufferSize());
    Thread.sleep(50);
    Assert.assertEquals(0, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 1
    leaderSink.transfer((TsFileInsertionEvent) mockTsFileEvents.get(1));
    Assert.assertEquals(3, leaderSink.getTransferBufferSize());
    Thread.sleep(50);
    Assert.assertEquals(1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 2
    leaderSink.transfer((TsFileInsertionEvent) mockTsFileEvents.get(2));
    Thread.sleep(50);
    Assert.assertEquals(2, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    // wait until receiver process all events
    Thread.sleep(5000);
    Assert.assertEquals(4, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(0, leaderSink.getTransferBufferSize());
  }

  @Test
  public void replicateWithConnectorReboot() throws Exception {
    PipeAgent.receiver().pipeConsensus().resetReceiver();
    // initialize WAL mock events
    prepareWALEvents(walEventSize, initialRebootTimes);

    for (int i = 0; i < mockWALEvents.size(); i++) {
      leaderSink.transfer((TabletInsertionEvent) mockWALEvents.get(i));
      Assert.assertEquals(1, leaderSink.getTransferBufferSize());
      Thread.sleep(50);

      Assert.assertEquals(i, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
      Assert.assertEquals(0, leaderSink.getRetryBufferSize());
      Assert.assertEquals(0, leaderSink.getTransferBufferSize());
    }
    Assert.assertEquals(initialRebootTimes, PipeAgent.receiver().pipeConsensus().getRebootTimes());

    // connector reboot
    mockWALEvents.clear();
    prepareWALEvents(walEventSize, initialRebootTimes + 1);

    for (int i = 0; i < mockWALEvents.size(); i++) {
      leaderSink.transfer((TabletInsertionEvent) mockWALEvents.get(i));
      Assert.assertEquals(1, leaderSink.getTransferBufferSize());
      Thread.sleep(100);

      Assert.assertEquals(i, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
      Assert.assertEquals(
          initialRebootTimes + 1, PipeAgent.receiver().pipeConsensus().getRebootTimes());
      Assert.assertEquals(0, leaderSink.getRetryBufferSize());
      Assert.assertEquals(0, leaderSink.getTransferBufferSize());
    }
  }

  @Test
  public void sequenceReplicateHybridTest() throws Exception {
    PipeAgent.receiver().pipeConsensus().resetReceiver();
    prepareHybridEvents();

    for (int i = 0; i < mockHybridEvents.size(); i++) {
      transferHybridEvent(mockHybridEvents.get(i));
      Assert.assertEquals(1, leaderSink.getTransferBufferSize());
      Thread.sleep(200);

      Assert.assertEquals(i, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
      Assert.assertEquals(0, leaderSink.getRetryBufferSize());
      Assert.assertEquals(0, leaderSink.getTransferBufferSize());
    }
  }

  @Test
  public void unsequenceReplicateHybridTest() throws Exception {
    PipeAgent.receiver().pipeConsensus().resetReceiver();
    prepareHybridEvents();

    // send 3
    transferHybridEvent(mockHybridEvents.get(3));
    Thread.sleep(50);
    Assert.assertEquals(-1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(1, leaderSink.getTransferBufferSize());
    // send 4
    transferHybridEvent(mockHybridEvents.get(4));
    Thread.sleep(50);
    Assert.assertEquals(-1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 0
    transferHybridEvent(mockHybridEvents.get(0));
    Assert.assertEquals(3, leaderSink.getTransferBufferSize());
    Thread.sleep(50);
    Assert.assertEquals(0, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 1
    transferHybridEvent(mockHybridEvents.get(1));
    Assert.assertEquals(3, leaderSink.getTransferBufferSize());
    Thread.sleep(50);
    Assert.assertEquals(1, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(2, leaderSink.getTransferBufferSize());
    // send 2
    transferHybridEvent(mockHybridEvents.get(2));
    Thread.sleep(50);
    Assert.assertEquals(2, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    // send 5
    transferHybridEvent(mockHybridEvents.get(5));
    // wait until receiver process all events
    Thread.sleep(5000 * 2);
    Assert.assertEquals(5, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(0, leaderSink.getTransferBufferSize());
  }

  @Test
  public void replicateWithReceiverReboot() throws Exception {
    PipeAgent.receiver().pipeConsensus().resetReceiver();
    prepareHybridEvents();

    // connector send 6 events
    for (int i = 0; i < mockHybridEvents.size(); i++) {
      transferHybridEvent(mockHybridEvents.get(i));
      Thread.sleep(100);

      if (i < 1) {
        Assert.assertEquals(i, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
        Assert.assertEquals(
            initialRebootTimes, PipeAgent.receiver().pipeConsensus().getRebootTimes());
        Assert.assertEquals(0, leaderSink.getRetryBufferSize());
        Assert.assertEquals(0, leaderSink.getTransferBufferSize());
      }

      // connector reboot
      if (i == 0) {
        PipeAgent.receiver().pipeConsensus().resetReceiver();
      }
    }
    Thread.sleep(100);

    // eventual consistency
    Assert.assertEquals(5, PipeAgent.receiver().pipeConsensus().getSyncCommitIndex());
    Assert.assertEquals(initialRebootTimes, PipeAgent.receiver().pipeConsensus().getRebootTimes());
    Assert.assertEquals(0, leaderSink.getRetryBufferSize());
    Assert.assertEquals(0, leaderSink.getTransferBufferSize());
  }

  private void transferHybridEvent(EnrichedEvent event) throws Exception {
    if (event instanceof TabletInsertionEvent) {
      leaderSink.transfer((TabletInsertionEvent) event);
    } else {
      leaderSink.transfer((TsFileInsertionEvent) event);
    }
  }

  /** prepare 6 hybrid events */
  private void prepareHybridEvents() {
    mockHybridEvents.clear();
    // prepare event 1
    mockHybridEvents.add(mockWALEvents.get(0));
    // prepare event 2
    mockHybridEvents.add(mockWALEvents.get(1));
    // prepare event 3
    mockHybridEvents.add(mockTsFileEvents.get(0));
    mockTsFileEvents.get(0).setCommitterKeyAndCommitId(committerKey, 2);
    // prepare event 4
    mockHybridEvents.add(mockWALEvents.get(2));
    Mockito.when(mockWALEvents.get(2).getCommitId()).thenReturn(3L);
    // prepare event 5
    mockHybridEvents.add(mockTsFileEvents.get(1));
    mockTsFileEvents.get(1).setCommitterKeyAndCommitId(committerKey, 4);
    // prepare event 6
    mockHybridEvents.add(mockTsFileEvents.get(2));
    mockTsFileEvents.get(2).setCommitterKeyAndCommitId(committerKey, 5);
  }

  private void prepareTsFileEvents(long startCommitId, int rebootTimes, int tsFileEventsSize)
      throws WALPipeException {
    mockTsFileEvents.clear();
    for (int i = 0; i < tsFileEventsSize; i++) {
      File tsFile = new File(tsFileDir, String.format("%s-%s-0-0.tsfile", i, i));
      try {
        boolean ignored1 = tsFile.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      TsFileResource resource = new TsFileResource(tsFile);
      resource.updateStartTime(
          new PlainDeviceID(String.join(TsFileConstant.PATH_SEPARATOR, device)), 0);

      PipeTsFileInsertionEvent pipeTsFileInsertionEvent =
          new PipeTsFileInsertionEvent(resource, false, false);
      pipeTsFileInsertionEvent.setRebootTimes(rebootTimes);
      pipeTsFileInsertionEvent.setCommitterKeyAndCommitId(committerKey, startCommitId + i);

      mockTsFileEvents.add(pipeTsFileInsertionEvent);
    }
  }

  private void prepareWALEvents(int rawTabletSize, int rebootTimes) throws WALPipeException {
    mockWALEvents.clear();
    for (int i = 0; i < rawTabletSize; i++) {
      PipeRawTabletInsertionEvent rawTabletInsertionEvent =
          Mockito.mock(PipeRawTabletInsertionEvent.class);
      Mockito.when(rawTabletInsertionEvent.getRebootTimes()).thenReturn(rebootTimes);
      Mockito.when(rawTabletInsertionEvent.getCommitId()).thenReturn((long) i);
      Mockito.when(rawTabletInsertionEvent.getCommitterKey()).thenReturn(committerKey);
      Mockito.when(rawTabletInsertionEvent.convertToTablet()).thenReturn(null);
      Mockito.when(rawTabletInsertionEvent.isAligned()).thenReturn(false);
      Mockito.when(rawTabletInsertionEvent.increaseReferenceCount(Mockito.anyString()))
          .thenReturn(true);

      mockWALEvents.add(rawTabletInsertionEvent);
    }
  }

  private void findPortAvailable(int i) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      try (ServerSocket ignored = new ServerSocket(this.peers.get(i).getEndpoint().port)) {
        // success
        return;
      } catch (IOException e) {
        // Port is already in use, wait and retry
        this.peers.set(i, new Peer(gid, i + 1, new TEndPoint("127.0.0.1", this.basePort)));
        logger.info("try port {} for node {}.", this.basePort++, i + 1);
        try {
          Thread.sleep(50); // Wait for 1 second before retrying
        } catch (InterruptedException ex) {
          // Handle the interruption if needed
        }
      }
    }
    Assert.fail(String.format("can not find port for node %d after 300s", i + 1));
  }
}
