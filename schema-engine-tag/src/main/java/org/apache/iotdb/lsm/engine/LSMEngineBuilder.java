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
package org.apache.iotdb.lsm.engine;

import org.apache.iotdb.lsm.context.DeleteRequestContext;
import org.apache.iotdb.lsm.context.InsertRequestContext;
import org.apache.iotdb.lsm.context.QueryRequestContext;
import org.apache.iotdb.lsm.levelProcess.LevelProcessChain;
import org.apache.iotdb.lsm.manager.DeletionManager;
import org.apache.iotdb.lsm.manager.InsertionManager;
import org.apache.iotdb.lsm.manager.QueryManager;
import org.apache.iotdb.lsm.manager.RecoverManager;
import org.apache.iotdb.lsm.manager.WALManager;
import org.apache.iotdb.lsm.request.IDeletionIRequest;
import org.apache.iotdb.lsm.request.IInsertionIRequest;
import org.apache.iotdb.lsm.request.IQueryIRequest;

public class LSMEngineBuilder<T> {

  private LSMEngine<T> lsmEngine;

  public LSMEngineBuilder() {
    lsmEngine = new LSMEngine<>();
  }

  public LSMEngineBuilder<T> buildWalManager(WALManager walManager) {
    lsmEngine.setWalManager(walManager);
    return this;
  }

  public <R extends IInsertionIRequest> LSMEngineBuilder<T> buildInsertionManager(
      LevelProcessChain<T, R, InsertRequestContext> levelProcessChain) {
    InsertionManager<T, R> insertionManager = new InsertionManager<>(lsmEngine.getWalManager());
    insertionManager.setLevelProcessChain(levelProcessChain);
    buildInsertionManager(insertionManager);
    return this;
  }

  public <R extends IInsertionIRequest> LSMEngineBuilder<T> buildInsertionManager(
      InsertionManager<T, R> insertionManager) {
    lsmEngine.setInsertionManager(insertionManager);
    return this;
  }

  public <R extends IDeletionIRequest> LSMEngineBuilder<T> buildDeletionManager(
      LevelProcessChain<T, R, DeleteRequestContext> levelProcessChain) {
    DeletionManager<T, R> deletionManager = new DeletionManager<>(lsmEngine.getWalManager());
    deletionManager.setLevelProcessChain(levelProcessChain);
    buildDeletionManager(deletionManager);
    return this;
  }

  public <R extends IDeletionIRequest> LSMEngineBuilder<T> buildDeletionManager(
      DeletionManager<T, R> deletionManager) {
    lsmEngine.setDeletionManager(deletionManager);
    return this;
  }

  public <R extends IQueryIRequest> LSMEngineBuilder<T> buildQueryManager(
      LevelProcessChain<T, R, QueryRequestContext> levelProcessChain) {
    QueryManager<T, R> queryManager = new QueryManager<>();
    queryManager.setLevelProcessChain(levelProcessChain);
    buildQueryManager(queryManager);
    return this;
  }

  public <R extends IQueryIRequest> LSMEngineBuilder<T> buildQueryManager(
      QueryManager<T, R> queryManager) {
    lsmEngine.setQueryManager(queryManager);
    return this;
  }

  public LSMEngineBuilder<T> buildRecoverManager(RecoverManager<LSMEngine<T>> recoverManager) {
    lsmEngine.setRecoverManager(recoverManager);
    return this;
  }

  public LSMEngineBuilder<T> buildRecoverManager() {
    RecoverManager<LSMEngine<T>> recoverManager = new RecoverManager<>(lsmEngine.getWalManager());
    lsmEngine.setRecoverManager(recoverManager);
    return this;
  }

  public LSMEngineBuilder<T> buildRootMemNode(T rootMemNode) {
    lsmEngine.setRootMemNode(rootMemNode);
    return this;
  }

  public LSMEngine<T> build() {
    return lsmEngine;
  }
}
