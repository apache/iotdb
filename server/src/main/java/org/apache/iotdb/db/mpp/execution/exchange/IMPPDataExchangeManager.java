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

package org.apache.iotdb.db.mpp.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ISinkChannel;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.mpp.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import java.util.List;

public interface IMPPDataExchangeManager {
  /**
   * Create a sink handle who sends data blocks to a remote downstream fragment instance in async
   * manner.
   *
   * @param localFragmentInstanceId ID of the local fragment instance who generates and sends data
   *     blocks to the sink handle.
   * @param instanceContext The context of local fragment instance.
   */
  ISinkHandle createShuffleSinkHandle(
      List<DownStreamChannelLocation> downStreamChannelLocationList,
      DownStreamChannelIndex downStreamChannelIndex,
      ShuffleSinkHandle.ShuffleStrategyEnum shuffleStrategyEnum,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      FragmentInstanceContext instanceContext);

  ISinkChannel createLocalSinkChannelForPipeline(DriverContext driverContext, String planNodeId);
  /**
   * Create a source handle who fetches data blocks from a remote upstream fragment instance for a
   * plan node of a local fragment instance in async manner.
   *
   * @param localFragmentInstanceId ID of the local fragment instance who receives data blocks from
   *     the source handle.
   * @param localPlanNodeId The local sink plan node ID.
   * @param remoteEndpoint Hostname and Port of the remote fragment instance where the data blocks
   *     should be received from.
   * @param remoteFragmentInstanceId ID of the remote fragment instance.
   * @param onFailureCallback The callback on failure.
   */
  ISourceHandle createSourceHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback);

  ISourceHandle createLocalSourceHandleForFragment(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      String remotePlanNodeId,
      TFragmentInstanceId remoteFragmentInstanceId,
      int index,
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback);

  /** SharedTsBlockQueue must belong to corresponding LocalSinkChannel */
  ISourceHandle createLocalSourceHandleForPipeline(SharedTsBlockQueue queue, DriverContext context);

  /**
   * Release all the related resources of a fragment instance, including data blocks that are not
   * yet fetched by downstream fragment instances.
   *
   * <p>This method should be called when a fragment instance finished in an abnormal state.
   *
   * @param fragmentInstanceId ID of the fragment instance to be released.
   */
  void forceDeregisterFragmentInstance(TFragmentInstanceId fragmentInstanceId);
}
