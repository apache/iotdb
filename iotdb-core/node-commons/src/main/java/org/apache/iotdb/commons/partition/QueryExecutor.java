/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.apache.tsfile.utils.Preconditions;

import java.util.Objects;

/** QueryExecutor indicates this query can execute directly without data from StorageEngine */
public class QueryExecutor implements ExecutorType {
  TDataNodeLocation dataNodeLocation;

  public QueryExecutor(TDataNodeLocation dataNodeLocation) {
    this.dataNodeLocation = dataNodeLocation;
  }

  @Override
  public TDataNodeLocation getDataNodeLocation() {
    Preconditions.checkArgument(dataNodeLocation != null);
    return dataNodeLocation;
  }

  @Override
  public boolean isStorageExecutor() {
    return false;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryExecutor that = (QueryExecutor) o;
    return Objects.equals(dataNodeLocation, that.dataNodeLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataNodeLocation);
  }
}
