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
import org.apache.iotdb.lsm.request.IDeletionRequest;
import org.apache.iotdb.lsm.request.IInsertionRequest;
import org.apache.iotdb.lsm.request.IQueryRequest;
import org.apache.iotdb.lsm.request.IRequest;

import java.io.IOException;

public class LSMEngine<T> implements ILSMEngine, IRecoverable {

  private InsertionManager<T, IInsertionRequest> insertionManager;

  private DeletionManager<T, IDeletionRequest> deletionManager;

  private QueryManager<T, IQueryRequest> queryManager;

  private WALManager walManager;

  private RecoverManager<LSMEngine<T>> recoverManager;

  private T rootMemNode;

  public LSMEngine() {}

  public void setRootMemNode(T rootMemNode) {
    this.rootMemNode = rootMemNode;
  }

  @Override
  public <K, V, R> void insert(IInsertionRequest<K, V, R> insertionRequest) {
    insertionManager.process(rootMemNode, insertionRequest, new InsertRequestContext());
  }

  @Override
  public <K, R> void query(IQueryRequest<K, R> queryRequest) {
    queryManager.process(rootMemNode, queryRequest, new QueryRequestContext());
  }

  @Override
  public <K, V, R> void delete(IDeletionRequest<K, V, R> deletionRequest) {
    deletionManager.process(rootMemNode, deletionRequest, new DeleteRequestContext());
  }

  @Override
  public void recover() {
    recoverManager.recover(this);
  }

  @Override
  public void clear() throws IOException {
    walManager.close();
  }

  protected <R extends IInsertionRequest> void setInsertionManager(
      InsertionManager<T, R> insertionManager) {
    this.insertionManager = (InsertionManager<T, IInsertionRequest>) insertionManager;
  }

  protected <R extends IDeletionRequest> void setDeletionManager(
      DeletionManager<T, R> deletionManager) {
    this.deletionManager = (DeletionManager<T, IDeletionRequest>) deletionManager;
  }

  protected <R extends IQueryRequest> void setQueryManager(QueryManager<T, R> queryManager) {
    this.queryManager = (QueryManager<T, IQueryRequest>) queryManager;
  }

  protected WALManager getWalManager() {
    return walManager;
  }

  protected void setWalManager(WALManager walManager) {
    this.walManager = walManager;
  }

  protected void setRecoverManager(RecoverManager<LSMEngine<T>> recoverManager) {
    this.recoverManager = recoverManager;
  }

  @Override
  public <K, V, R> void recover(IRequest<K, V, R> request) {
    switch (request.getRequestType()) {
      case INSERT:
        insert((IInsertionRequest<K, V, R>) request);
        break;
      case DELETE:
        delete((IDeletionRequest<K, V, R>) request);
        break;
      default:
        break;
    }
  }
}
