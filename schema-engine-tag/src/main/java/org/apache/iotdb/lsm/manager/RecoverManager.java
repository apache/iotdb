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

import org.apache.iotdb.lsm.engine.IRecoverable;
import org.apache.iotdb.lsm.request.IRequest;

/** for memory structure recovery */
public class RecoverManager<T extends IRecoverable> {

  private WALManager walManager;

  public RecoverManager(WALManager walManager) {
    this.walManager = walManager;
    walManager.setRecover(true);
  }

  /**
   * recover
   *
   * @param t extends IRecoverable
   */
  public void recover(T t) {
    while (true) {
      IRequest request = walManager.read();
      if (request == null) {
        walManager.setRecover(false);
        return;
      }
      t.recover(request);
    }
  }
}
