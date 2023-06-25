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

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

/** The interface is used to indicate where to execute a FragmentInstance */
public interface ExecutorType {

  /** Indicate if ExecutorType is StorageExecutor */
  boolean isStorageExecutor();

  TDataNodeLocation getDataNodeLocation();

  default TRegionReplicaSet getRegionReplicaSet() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Try to update the preferred location to the given EndPoint in the ReplicaSet. Do nothing if the
   * operation is not supported or the EndPoint is not found within this ReplicaSet.
   *
   * @param endPoint associated with the preferred location.
   */
  default void updatePreferredLocation(TEndPoint endPoint) {}
}
