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
import org.apache.iotdb.lsm.manager.DeletionManager;
import org.apache.iotdb.lsm.manager.InsertionManager;
import org.apache.iotdb.lsm.manager.QueryManager;
import org.apache.iotdb.lsm.manager.RecoverManager;
import org.apache.iotdb.lsm.manager.WALManager;
import org.apache.iotdb.lsm.recover.IRecoverable;
import org.apache.iotdb.lsm.request.DeletionRequest;
import org.apache.iotdb.lsm.request.InsertionRequest;
import org.apache.iotdb.lsm.request.QueryRequest;
import org.apache.iotdb.lsm.request.Request;

import java.io.IOException;

public class LSMEngine<T> implements ILSMEngine, IRecoverable {

  private InsertionManager<T, InsertionRequest> insertionManager;

  private DeletionManager<T, DeletionRequest> deletionManager;

  private QueryManager<T, QueryRequest> queryManager;

  private WALManager walManager;

  private RecoverManager<LSMEngine<T>> recoverManager;

  private T rootMemNode;

  public LSMEngine() {}

  public LSMEngine(WALManager walManager) throws Exception {
    this.walManager = walManager;
    insertionManager = new InsertionManager<>(walManager);
    deletionManager = new DeletionManager<>(walManager);
    queryManager = new QueryManager<>();
    recoverManager = new RecoverManager<>(walManager);
    recoverManager.recover(this);
  }

  public T getRootMemNode() {
    return rootMemNode;
  }

  public void setRootMemNode(T rootMemNode) {
    this.rootMemNode = rootMemNode;
  }

  @Override
  public <K, V, R> void insert(InsertionRequest<K, V, R> insertionRequest) throws Exception {
    insertionManager.process(rootMemNode, insertionRequest, new InsertRequestContext());
  }

  @Override
  public <K, V, R> void insert(
      InsertionRequest<K, V, R> insertionRequest, InsertRequestContext insertRequestContext)
      throws Exception {
    insertionManager.process(rootMemNode, insertionRequest, insertRequestContext);
  }

  @Override
  public <K, R> void query(QueryRequest<K, R> queryRequest) throws Exception {
    queryManager.process(rootMemNode, queryRequest, new QueryRequestContext());
  }

  @Override
  public <K, R> void query(QueryRequest<K, R> queryRequest, QueryRequestContext queryRequestContext)
      throws Exception {
    queryManager.process(rootMemNode, queryRequest, queryRequestContext);
  }

  @Override
  public <K, V, R> void delete(DeletionRequest<K, V, R> deletionRequest) throws Exception {
    deletionManager.process(rootMemNode, deletionRequest, new DeleteRequestContext());
  }

  @Override
  public <K, V, R> void delete(
      DeletionRequest<K, V, R> deletionRequest, DeleteRequestContext deleteRequestContext)
      throws Exception {
    deletionManager.process(rootMemNode, deletionRequest, deleteRequestContext);
  }

  @Override
  public void recover() throws Exception {
    recoverManager.recover(this);
  }

  @Override
  public void clear() throws IOException {
    walManager.close();
  }

  public InsertionManager<T, InsertionRequest> getInsertionManager() {
    return insertionManager;
  }

  public <R extends InsertionRequest> void setInsertionManager(
      InsertionManager<T, R> insertionManager) {
    this.insertionManager = (InsertionManager<T, InsertionRequest>) insertionManager;
  }

  public DeletionManager<T, DeletionRequest> getDeletionManager() {
    return deletionManager;
  }

  public <R extends DeletionRequest> void setDeletionManager(
      DeletionManager<T, R> deletionManager) {
    this.deletionManager = (DeletionManager<T, DeletionRequest>) deletionManager;
  }

  public QueryManager<T, QueryRequest> getQueryManager() {
    return queryManager;
  }

  public <R extends QueryRequest> void setQueryManager(QueryManager<T, R> queryManager) {
    this.queryManager = (QueryManager<T, QueryRequest>) queryManager;
  }

  public WALManager getWalManager() {
    return walManager;
  }

  public void setWalManager(WALManager walManager) {
    this.walManager = walManager;
  }

  public RecoverManager<LSMEngine<T>> getRecoverManager() {
    return recoverManager;
  }

  public void setRecoverManager(RecoverManager<LSMEngine<T>> recoverManager) {
    this.recoverManager = recoverManager;
  }

  @Override
  public <K, V, R> void recover(Request<K, V, R> request) throws Exception {
    switch (request.getRequestType()) {
      case INSERT:
        insert((InsertionRequest<K, V, R>) request);
        break;
      case DELETE:
        delete((DeletionRequest<K, V, R>) request);
        break;
      default:
        break;
    }
  }
}
