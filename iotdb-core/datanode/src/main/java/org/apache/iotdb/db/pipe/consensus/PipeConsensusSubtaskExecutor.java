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
package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

public class PipeConsensusSubtaskExecutor extends PipeSinkSubtaskExecutor {
  public PipeConsensusSubtaskExecutor() {
    super(
        DataRegion.getAcquireDirectBufferMemCost() == 0
            ? Runtime.getRuntime().availableProcessors()
            :
            // The number of data regions for a datanode is limited by offHeapMemory. At the same
            // time,
            // in order to ensure multicore performance, the number of data regions usually does not
            // exceed the number of cores. To prevent the thread from exploding, we take the min of
            // both.
            (int)
                Math.min(
                    // NOTE1: The number of data regions is limited by the number of cores.
                    Runtime.getRuntime().availableProcessors(),
                    // NOTE2: The number of data regions is also limited by the offHeapMemory. In
                    // fact,
                    // schema regions also take up off-heap memory. So in fact, the soft cap of
                    // data region will be smaller than what is calculated here, but that's okay, we
                    // can
                    // set the core size quota of the Executor pool a little bit higher slightly
                    Math.max(
                            IoTDBDescriptor.getInstance()
                                .getMemoryConfig()
                                .getDirectBufferMemoryManager()
                                .getTotalMemorySizeInBytes(),
                            IoTDBDescriptor.getInstance()
                                .getMemoryConfig()
                                .getOffHeapMemoryManager()
                                .getTotalMemorySizeInBytes())
                        / DataRegion.getAcquireDirectBufferMemCost()),
        ThreadName.PIPE_CONSENSUS_EXECUTOR_POOL.getName());
  }
}
