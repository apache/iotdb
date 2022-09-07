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
package org.apache.iotdb.lsm.context;

import org.apache.iotdb.lsm.strategy.BFSAccessStrategy;
import org.apache.iotdb.lsm.strategy.RBFSAccessStrategy;

public class FlushContext extends Context {

  // 最小的已经flush的层级
  int minimumFlushedLevel;

  public FlushContext() {
    super();
    type = ContextType.FLUSH;
    accessStrategy = new RBFSAccessStrategy();
    minimumFlushedLevel = Integer.MAX_VALUE;
  }

  public int getMinimumFlushedLevel() {
    return minimumFlushedLevel;
  }

  public void setMinimumFlushedLevel(int minimumFlushedLevel) {
    this.minimumFlushedLevel = minimumFlushedLevel;
  }
}
