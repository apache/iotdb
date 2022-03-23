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

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;
import org.apache.iotdb.mpp.rpc.thrift.ThriftFragmentInstanceId;

public interface IDataBlockManager {
  /**
   * Create a sink handle who sends data blocks to a remote downstream fragment instance in async
   * manner.
   */
  ISinkHandle createSinkHandle(
      ThriftFragmentInstanceId localFragmentInstanceId,
      String remoteHostname,
      ThriftFragmentInstanceId remoteFragmentInstanceId,
      String remoteOperatorId);

  /**
   * Create a source handle who fetches data blocks from a remote upstream fragment instance for an
   * operator of a local fragment instance in async manner.
   */
  ISourceHandle createSourceHandle(
      ThriftFragmentInstanceId localFragmentInstanceId,
      String localOperatorId,
      String remoteHostname,
      ThriftFragmentInstanceId remoteFragmentInstanceId);

  /**
   * Release all the related resources, including data blocks that are not yet fetched by downstream
   * fragment instances.
   *
   * <p>This method should be called when a fragment instance finished in an abnormal state.
   */
  void forceDeregisterFragmentInstance(FragmentInstanceTask task);
}
