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

import java.io.IOException;

/**
 * This interface defines the appearance of the LSM framework and provides read and write methods
 */
public interface ILSMEngine {

  /**
   * @param insertionRequest
   * @param <K>
   * @param <V>
   * @param <R>
   */
  <K, V, R> void insert(IInsertionRequest<K, V, R> insertionRequest);

  <K, R> void query(IQueryRequest<K, R> queryRequest);

  <K, V, R> void delete(IDeletionRequest<K, V, R> deletionRequest);

  void recover();

  @TestOnly
  void clear() throws IOException;
}
