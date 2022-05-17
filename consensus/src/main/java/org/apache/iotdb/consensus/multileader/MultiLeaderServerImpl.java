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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.multileader.asyncLogAppender.AsyncLogAppender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class MultiLeaderServerImpl {

  private final Logger logger = LoggerFactory.getLogger(MultiLeaderServerImpl.class);
  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final String storageDir;
  private List<Peer> configuration;
  private IndexController currentNodeController;
  private List<AsyncLogAppender> asyncLogAppenders;

  private static final int DEFAULT_BUFFER_SIZE = 1024 * 2;

  public MultiLeaderServerImpl(
      String storageDir, Peer thisNode, List<Peer> configuration, IStateMachine stateMachine) {
    this.storageDir = storageDir;
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.currentNodeController =
        new IndexController(storageDir, Utils.fromTEndPointToString(thisNode.getEndpoint()), false);
    this.configuration = configuration;
    persistConfiguration();
    //    configuration.stream().filter(x -> !Objects.equals(x, thisNode)).

  }

  public MultiLeaderServerImpl(String storageDir, Peer thisNode, IStateMachine stateMachine) {
    this.storageDir = storageDir;
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.currentNodeController =
        new IndexController(storageDir, Utils.fromTEndPointToString(thisNode.getEndpoint()), false);
    this.configuration = new ArrayList<>();
    recoverConfiguration();
  }

  public IStateMachine getStateMachine() {
    return stateMachine;
  }

  public void start() {
    stateMachine.start();
  }

  public void stop() {
    stateMachine.stop();
  }

  public TSStatus write(IConsensusRequest request) {
    synchronized (stateMachine) {
      IndexedConsensusRequest newRequest =
          new IndexedConsensusRequest(
              Long.MAX_VALUE, currentNodeController.incrementAndGet(), request);
      return stateMachine.write(newRequest);
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
    ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    buffer.putInt(configuration.size());
    for (Peer peer : configuration) {
      peer.serialize(buffer);
    }
    try {
      Files.write(
          Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath()),
          buffer.array());
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
}
