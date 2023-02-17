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
import org.apache.iotdb.lsm.request.IDeletionRequest;
import org.apache.iotdb.lsm.request.IInsertionRequest;
import org.apache.iotdb.lsm.request.IQueryRequest;
import org.apache.iotdb.lsm.response.IResponse;

import java.io.IOException;

/**
 * This interface defines the appearance of the LSM framework and provides read and write methods
 */
public interface ILSMEngine extends IRecoverable {

  /**
   * Use this ILSMEngine to insert data
   *
   * @param insertionRequest Encapsulates the data to be inserted
   * @param <K> The type of key in the request data
   * @param <V> The type of value in the request data
   * @param <R> type of response
   */
  <K, V, R extends IResponse> R insert(IInsertionRequest<K, V> insertionRequest);

  /**
   * Use this ILSMEngine to query
   *
   * @param queryRequest Encapsulates query data
   * @param <K> The type of key in the request data
   * @param <R> type of response
   */
  <K, R extends IResponse> R query(IQueryRequest<K> queryRequest);

  /**
   * Use this ILSMEngine to delete data
   *
   * @param deletionRequest Encapsulates the data to be deleted
   * @param <K> The type of key in the request data
   * @param <V> The type of value in the request data
   * @param <R> type of response
   */
  <K, V, R extends IResponse> R delete(IDeletionRequest<K, V> deletionRequest);

  /** recover the ILSMEngine */
  void recover();

  /**
   * Close all open resources
   *
   * @throws IOException
   */
  @TestOnly
  void clear() throws IOException;
}
