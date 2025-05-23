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

package org.apache.iotdb.db.pipe.resource.memory.strategy;

import org.apache.iotdb.db.pipe.resource.memory.PipeDynamicMemoryBlock;

// Now let's define the operation memory behavior: Producers produce memory, consumers consume
// memory, and in order to ensure that consumers do not encounter back pressure, the memory that
// consumers need to use is allocated in advance. Consumer instances obtain their expected memory
// through allocation strategies, and the total memory of all consumer instances must not be greater
// than the pre-allocated memory. The memory allocation algorithm is to adjust the memory of
// consumers so that the consumption rate can reach the optimal
public interface DynamicMemoryAllocationStrategy {

  void dynamicallyAdjustMemory(PipeDynamicMemoryBlock dynamicMemoryBlock);
}
