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

package org.apache.iotdb.db.mpp.execution.memory;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

/**
 * Manages memory of a data node. The memory is divided into two memory pools so that the memory for
 * read and for write can be isolated.
 */
public class LocalMemoryManager {

  private final MemoryPool queryPool;

  public LocalMemoryManager() {
    queryPool =
        new MemoryPool(
            "query",
            IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForDataExchange(),
            IoTDBDescriptor.getInstance().getConfig().getMaxBytesPerQuery());
  }

  public MemoryPool getQueryPool() {
    return queryPool;
  }
}
