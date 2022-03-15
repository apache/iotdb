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

package org.apache.iotdb.mpp.shuffle;

import org.apache.iotdb.mpp.common.ITSBlock;
import org.apache.iotdb.mpp.execution.task.FragmentInstanceID;
import org.apache.iotdb.mpp.execution.task.FragmentInstanceTask;

public interface IDataBlockManager {

  /**
   * Register a new fragment instance. The block manager will start looking for upstream data blocks
   * and flushing data blocks generated to downstream fragment instances.
   */
  void registerFragmentInstance(FragmentInstanceTask task);

  /**
   * Deregister a fragment instance. The block manager will stop looking for upstream data blocks
   * and release the input data blocks, but will keep flushing data blocks to downstream fragment
   * instances until all the data blocks are sent. Once all the data blocks are sent, the output
   * data blocks will be release.
   *
   * <p>This method should be called when a fragment instance finished in a normal state.
   */
  void deregisterFragmentInstance(FragmentInstanceTask task);

  /**
   * Deregister a fragment instance. The block manager will release all the related resources.
   * Including data blocks that are not yet sent to downstream fragment instances.
   *
   * <p>This method should be called when a fragment instance finished in an abnormal state.
   */
  void forceDeregisterFragmentInstance(FragmentInstanceTask task);

  /**
   * Put a data block to the output buffer for downstream fragment instances. Will throw an {@link
   * IllegalStateException} if the output buffer is full.
   *
   * <p>Once the block be put into the output buffer, the data block manager will notify downstream
   * fragment instances that a new data block is available.
   *
   * @param instanceID ID of fragment instance that generates the block.
   * @return If there are enough memory for the next block.
   */
  boolean putDataBlock(FragmentInstanceID instanceID, ITSBlock block);

  /**
   * Check if there are data blocks from the specified upstream fragment instance.
   *
   * @param instanceID ID of the upstream fragment instance.
   * @return If there are available data blocks.
   */
  boolean hasDataBlock(FragmentInstanceID instanceID);

  /**
   * Get a data block from the input buffer of specified upstream fragment instance. Will throw an
   * {@link IllegalStateException} if the input buffer is empty.
   *
   * @param instanceID ID of the upstream fragment instance.
   * @return A data block.
   */
  ITSBlock getDataBlock(FragmentInstanceID instanceID);
}
