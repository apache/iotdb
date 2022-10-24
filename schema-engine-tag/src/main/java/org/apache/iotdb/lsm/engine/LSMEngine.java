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
import org.apache.iotdb.lsm.request.IDeletionIRequest;
import org.apache.iotdb.lsm.request.IInsertionIRequest;
import org.apache.iotdb.lsm.request.IQueryIRequest;
import org.apache.iotdb.lsm.request.IRequest;

import java.io.IOException;

public class LSMEngine<T> implements ILSMEngine, IRecoverable {

  private InsertionManager<T, IInsertionIRequest> insertionManager;

  private DeletionManager<T, IDeletionIRequest> deletionManager;

  private QueryManager<T, IQueryIRequest> queryManager;

  private WALManager walManager;

  private RecoverManager<LSMEngine<T>> recoverManager;

  private T rootMemNode;

  public LSMEngine() {}

  public void setRootMemNode(T rootMemNode) {
    this.rootMemNode = rootMemNode;
  }

  @Override
  public <K, V, R> void insert(IInsertionIRequest<K, V, R> insertionRequest) {
    insertionManager.process(rootMemNode, insertionRequest, new InsertRequestContext());
  }

  @Override
  public <K, R> void query(IQueryIRequest<K, R> queryRequest) {
    queryManager.process(rootMemNode, queryRequest, new QueryRequestContext());
  }

  @Override
  public <K, V, R> void delete(IDeletionIRequest<K, V, R> deletionRequest) {
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

  protected <R extends IInsertionIRequest> void setInsertionManager(
      InsertionManager<T, R> insertionManager) {
    this.insertionManager = (InsertionManager<T, IInsertionIRequest>) insertionManager;
  }

  protected <R extends IDeletionIRequest> void setDeletionManager(
      DeletionManager<T, R> deletionManager) {
    this.deletionManager = (DeletionManager<T, IDeletionIRequest>) deletionManager;
  }

  protected <R extends IQueryIRequest> void setQueryManager(QueryManager<T, R> queryManager) {
    this.queryManager = (QueryManager<T, IQueryIRequest>) queryManager;
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
        insert((IInsertionIRequest<K, V, R>) request);
        break;
      case DELETE:
        delete((IDeletionIRequest<K, V, R>) request);
        break;
      default:
        break;
    }
  }
}
