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

import org.apache.iotdb.lsm.request.IRequest;

/** Any object implements this interface can be recovered by RecoverManager */
public interface IRecoverable {

  /**
   * Use Request to recover the object which implements the interface
   *
   * @param request insertionRequest or deletionRequest
   * @param <K> The type of key in the request data
   * @param <V> The type of value in the request data
   */
  <K, V> void recover(IRequest<K, V> request);
}
