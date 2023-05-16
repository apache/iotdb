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

import java.util.List;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import javax.annotation.Nonnull;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StorageExecutor indicates execution of this query need data from StorageEngine
 */
public class StorageExecutor implements ExecutorType {

  private static final Logger logger = LoggerFactory.getLogger(StorageExecutor.class);
  private final TRegionReplicaSet regionReplicaSet;

  public StorageExecutor(@Nonnull TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public TDataNodeLocation getDataNodeLocation() {
    if (regionReplicaSet.isSetPreferredLocation()) {
      int preferredLocation = regionReplicaSet.getPreferredLocation();
      if (preferredLocation >= 0
          && preferredLocation < regionReplicaSet.getDataNodeLocationsSize()) {
        return regionReplicaSet.getDataNodeLocations().get(preferredLocation);
      }
    }
    return regionReplicaSet.getDataNodeLocations().get(0);
  }

  @Override
  public boolean isStorageExecutor() {
    return true;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StorageExecutor that = (StorageExecutor) o;
    return Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionReplicaSet);
  }

  @Override
  public void updatePreferredLocation(TEndPoint endPoint) {
    List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
    int i = 0;
    for (; i < dataNodeLocations.size(); i++) {
      TDataNodeLocation dataNodeLocation = dataNodeLocations.get(i);
      if (Objects.equals(dataNodeLocation.getClientRpcEndPoint(), endPoint)) {
        break;
      }
      if (Objects.equals(dataNodeLocation.getDataRegionConsensusEndPoint(), endPoint)) {
        break;
      }
      if (Objects.equals(dataNodeLocation.getSchemaRegionConsensusEndPoint(), endPoint)) {
        break;
      }
      if (Objects.equals(dataNodeLocation.getMPPDataExchangeEndPoint(), endPoint)) {
        break;
      }
      if (Objects.equals(dataNodeLocation.getInternalEndPoint(), endPoint)) {
        break;
      }
    }

    if (i < dataNodeLocations.size()) {
      regionReplicaSet.setPreferredLocation(i);
      logger.info("Preferred location of {} has been set to {}", regionReplicaSet,
          regionReplicaSet.getDataNodeLocations().get(i));
    }
  }
}
