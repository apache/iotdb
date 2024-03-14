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

package org.apache.iotdb.consensus.simple;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleConsensusServerImpl implements IStateMachine {

  private final Peer peer;
  private final IStateMachine stateMachine;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public SimpleConsensusServerImpl(Peer peer, IStateMachine stateMachine) {
    this.peer = peer;
    this.stateMachine = stateMachine;
  }

  public Peer getPeer() {
    return peer;
  }

  public IStateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public void start() {
    lock.writeLock().lock();
    try {
      if (initialized.compareAndSet(false, true)) {
        stateMachine.start();
        // Notify itself as the leader
        stateMachine.event().notifyLeaderChanged(peer.getGroupId(), peer.getNodeId());
        stateMachine.event().notifyLeaderReady();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void stop() {
    lock.writeLock().lock();
    try {
      stateMachine.stop();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean isReadOnly() {
    lock.readLock().lock();
    try {
      return stateMachine.isReadOnly();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    lock.writeLock().lock();
    try {
      return stateMachine.write(request);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public IConsensusRequest deserializeRequest(IConsensusRequest request) {
    return request;
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    lock.readLock().lock();
    try {
      return stateMachine.read(request);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    lock.writeLock().lock();
    try {
      return stateMachine.takeSnapshot(snapshotDir);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    lock.writeLock().lock();
    try {
      stateMachine.loadSnapshot(latestSnapshotRootDir);
    } finally {
      lock.writeLock().unlock();
    }
  }
}
