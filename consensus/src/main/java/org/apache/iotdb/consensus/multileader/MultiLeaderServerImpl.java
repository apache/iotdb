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

package org.apache.iotdb.consensus.multileader;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.MultiLeaderConfig;
import org.apache.iotdb.consensus.multileader.client.AsyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.multileader.wal.ConsensusReqReader;
import org.apache.iotdb.consensus.multileader.wal.GetConsensusReqReaderPlan;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MultiLeaderServerImpl {

  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";

  private final Logger logger = LoggerFactory.getLogger(MultiLeaderServerImpl.class);

  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final String storageDir;
  private final List<Peer> configuration;
  private final AtomicLong index;
  private final LogDispatcher logDispatcher;
  private final MultiLeaderConfig config;

  public MultiLeaderServerImpl(
      String storageDir,
      Peer thisNode,
      List<Peer> configuration,
      IStateMachine stateMachine,
      IClientManager<TEndPoint, AsyncMultiLeaderServiceClient> clientManager,
      MultiLeaderConfig config) {
    this.storageDir = storageDir;
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.configuration = configuration;
    if (configuration.isEmpty()) {
      recoverConfiguration();
    } else {
      persistConfiguration();
    }
    this.config = config;
    this.logDispatcher = new LogDispatcher(this, clientManager);
    // restart
    ConsensusReqReader reader =
        (ConsensusReqReader) stateMachine.read(new GetConsensusReqReaderPlan());
    long currentSearchIndex = reader.getCurrentSearchIndex();
    this.index = new AtomicLong(currentSearchIndex);
  }

  public IStateMachine getStateMachine() {
    return stateMachine;
  }

  public void start() {
    stateMachine.start();
    logDispatcher.start();
  }

  public void stop() {
    logDispatcher.stop();
    stateMachine.stop();
  }

  /**
   * records the index of the log and writes locally, and then asynchronous replication is performed
   */
  public TSStatus write(IConsensusRequest request) {
    synchronized (stateMachine) {
      IndexedConsensusRequest indexedConsensusRequest =
          buildIndexedConsensusRequestForLocalRequest(request);
      if (indexedConsensusRequest.getSearchIndex() % 1000 == 0) {
        logger.info(
            "DataRegion[{}]: index after build: safeIndex: {}, searchIndex: {}",
            thisNode.getGroupId(),
            indexedConsensusRequest.getSafelyDeletedSearchIndex(),
            indexedConsensusRequest.getSearchIndex());
      }
      // TODO wal and memtable
      TSStatus result = stateMachine.write(indexedConsensusRequest);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        logDispatcher.offer(indexedConsensusRequest);
      } else {
        logger.debug(
            "{}: write operation failed. searchIndex: {}. Code: {}",
            thisNode.getGroupId(),
            indexedConsensusRequest.getSearchIndex(),
            result.getCode());
        index.decrementAndGet();
      }

      return result;
    }
  }

  public DataSet read(IConsensusRequest request) {
    return stateMachine.read(request);
  }

  public boolean takeSnapshot(File snapshotDir) {
    return stateMachine.takeSnapshot(snapshotDir);
  }

  public void loadSnapshot(File latestSnapshotRootDir) {
    stateMachine.loadSnapshot(latestSnapshotRootDir);
  }

  public void persistConfiguration() {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
      outputStream.writeInt(configuration.size());
      for (Peer peer : configuration) {
        peer.serialize(outputStream);
      }
      Files.write(
          Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath()),
          publicBAOS.getBuf());
    } catch (IOException e) {
      logger.error("Unexpected error occurs when persisting configuration", e);
    }
  }

  public void recoverConfiguration() {
    ByteBuffer buffer;
    try {
      buffer =
          ByteBuffer.wrap(
              Files.readAllBytes(
                  Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath())));
      int size = buffer.getInt();
      for (int i = 0; i < size; i++) {
        configuration.add(Peer.deserialize(buffer));
      }
    } catch (IOException e) {
      logger.error("Unexpected error occurs when recovering configuration", e);
    }
  }

  public IndexedConsensusRequest buildIndexedConsensusRequestForLocalRequest(
      IConsensusRequest request) {
    return new IndexedConsensusRequest(
        index.incrementAndGet(), getCurrentSafelyDeletedSearchIndex(), request);
  }

  public IndexedConsensusRequest buildIndexedConsensusRequestForRemoteRequest(
      ByteBufferConsensusRequest request) {
    return new IndexedConsensusRequest(
        ConsensusReqReader.DEFAULT_SEARCH_INDEX, getCurrentSafelyDeletedSearchIndex(), request);
  }

  /**
   * In the case of multiple copies, the minimum synchronization index is selected. In the case of
   * single copies, the current index is selected
   */
  public long getCurrentSafelyDeletedSearchIndex() {
    return logDispatcher.getMinSyncIndex().orElseGet(index::get);
  }

  public String getStorageDir() {
    return storageDir;
  }

  public Peer getThisNode() {
    return thisNode;
  }

  public List<Peer> getConfiguration() {
    return configuration;
  }

  public long getIndex() {
    return index.get();
  }

  public MultiLeaderConfig getConfig() {
    return config;
  }
}
