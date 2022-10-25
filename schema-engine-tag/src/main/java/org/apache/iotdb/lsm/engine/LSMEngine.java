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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.lsm.context.requestcontext.DeleteRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.InsertRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.QueryRequestContext;
import org.apache.iotdb.lsm.manager.DeletionManager;
import org.apache.iotdb.lsm.manager.InsertionManager;
import org.apache.iotdb.lsm.manager.QueryManager;
import org.apache.iotdb.lsm.manager.RecoverManager;
import org.apache.iotdb.lsm.manager.WALManager;
import org.apache.iotdb.lsm.request.IDeletionRequest;
import org.apache.iotdb.lsm.request.IInsertionRequest;
import org.apache.iotdb.lsm.request.IQueryRequest;
import org.apache.iotdb.lsm.request.IRequest;

import java.io.IOException;

/**
 * The default ILSMEngine implementation class provided by the LSM framework
 *
 * @param <T> The type of root memory node handled by this engine
 */
public class LSMEngine<T> implements ILSMEngine {

  // Use the framework's default InsertionManager object to handle insert requests
  private InsertionManager<T, IInsertionRequest> insertionManager;

  // Use the framework's default DeletionManager object to handle delete requests
  private DeletionManager<T, IDeletionRequest> deletionManager;

  // Use the framework's default QueryManager object to handle query requests
  private QueryManager<T, IQueryRequest> queryManager;

  // Used to manage wal logs
  private WALManager walManager;

  // Use the framework's default RecoverManager object to recover the LSMEngine
  private RecoverManager<LSMEngine<T>> recoverManager;

  // Managed root memory node
  private T rootMemNode;

  public LSMEngine() {}

  /**
   * Use this LSMEngine to insert data
   *
   * @param insertionRequest Encapsulates the data to be inserted
   * @param <K> The type of key in the request data
   * @param <V> The type of value in the request data
   * @param <R> return value type after insertion
   */
  @Override
  public <K, V, R> void insert(IInsertionRequest<K, V, R> insertionRequest) {
    insertionManager.process(rootMemNode, insertionRequest, new InsertRequestContext());
  }

  /**
   * Use this LSMEngine to query
   *
   * @param queryRequest Encapsulates query data
   * @param <K> The type of key in the request data
   * @param <R> return value type after query
   */
  @Override
  public <K, R> void query(IQueryRequest<K, R> queryRequest) {
    queryManager.process(rootMemNode, queryRequest, new QueryRequestContext());
  }

  /**
   * Use this LSMEngine to delete data
   *
   * @param deletionRequest Encapsulates the data to be deleted
   * @param <K> The type of key in the request data
   * @param <V> The type of value in the request data
   * @param <R> return value type after deletion
   */
  @Override
  public <K, V, R> void delete(IDeletionRequest<K, V, R> deletionRequest) {
    deletionManager.process(rootMemNode, deletionRequest, new DeleteRequestContext());
  }

  /** recover the LSMEngine */
  @Override
  public void recover() {
    recoverManager.recover(this);
  }

  /**
   * Close all open resources
   *
   * @throws IOException
   */
  @Override
  @TestOnly
  public void clear() throws IOException {
    walManager.close();
  }

  /**
   * Use Request to recover the LSMEngine
   *
   * @param request insertionRequest or deletionRequest
   * @param <K> The type of key in the request data
   * @param <V> The type of value in the request data
   * @param <R> return value type after deletion
   */
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

  protected void setRootMemNode(T rootMemNode) {
    this.rootMemNode = rootMemNode;
  }
}
