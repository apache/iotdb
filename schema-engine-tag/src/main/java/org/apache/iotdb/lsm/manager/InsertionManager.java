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
package org.apache.iotdb.lsm.manager;

import org.apache.iotdb.lsm.context.requestcontext.InsertRequestContext;
import org.apache.iotdb.lsm.request.IInsertionRequest;

/** manage insertion to root memory node */
public class InsertionManager<T, R extends IInsertionRequest>
    extends BasicLSMManager<T, R, InsertRequestContext> {

  // use wal manager object to write wal file on insertion
  private WALManager walManager;

  public InsertionManager(WALManager walManager) {
    this.walManager = walManager;
  }

  /**
   * write wal file on insertion
   *
   * @param root root memory node
   * @param context insert request context
   */
  @Override
  public void preProcess(T root, R insertionRequest, InsertRequestContext context) {
    walManager.write(insertionRequest);
  }

  @Override
  public void postProcess(T root, R request, InsertRequestContext context) {}
}
