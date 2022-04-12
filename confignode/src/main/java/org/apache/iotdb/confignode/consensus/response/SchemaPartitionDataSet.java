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

package org.apache.iotdb.confignode.consensus.response;

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaPartitionDataSet implements DataSet {

  private TSStatus status;

  private SchemaPartition schemaPartition;

  public SchemaPartitionDataSet() {
    // empty constructor
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public void setSchemaPartition(SchemaPartition schemaPartition) {
    this.schemaPartition = schemaPartition;
  }

  public void convertToRpcSchemaPartitionResp(TSchemaPartitionResp resp) {
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap = new HashMap<>();
    resp.setStatus(status);

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      schemaPartition
          .getSchemaPartitionMap()
          .forEach(
              (storageGroup, seriesPartitionSlotRegionReplicaSetMap) -> {
                // Extract StorageGroupName
                schemaPartitionMap.putIfAbsent(storageGroup, new HashMap<>());

                // Extract Map<SeriesPartitionSlot, RegionReplicaSet>
                seriesPartitionSlotRegionReplicaSetMap.forEach(
                    ((seriesPartitionSlot, regionReplicaSet) -> {
                      // Extract TSeriesPartitionSlot
                      TSeriesPartitionSlot tSeriesPartitionSlot =
                          new TSeriesPartitionSlot(seriesPartitionSlot.getSlotId());

                      // Extract TRegionReplicaSet
                      TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();
                      // Set TRegionReplicaSet's RegionId
                      ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES);
                      regionReplicaSet.getConsensusGroupId().serializeImpl(buffer);
                      buffer.flip();
                      tRegionReplicaSet.setRegionId(buffer);
                      // Set TRegionReplicaSet's EndPoints
                      List<EndPoint> endPointList = new ArrayList<>();
                      regionReplicaSet
                          .getDataNodeList()
                          .forEach(
                              dataNodeLocation ->
                                  endPointList.add(
                                      new EndPoint(
                                          dataNodeLocation.getEndPoint().getIp(),
                                          dataNodeLocation.getEndPoint().getPort())));
                      tRegionReplicaSet.setEndpoint(endPointList);
                      schemaPartitionMap
                          .get(storageGroup)
                          .put(tSeriesPartitionSlot, tRegionReplicaSet);
                    }));
              });
    }

    resp.setSchemaRegionMap(schemaPartitionMap);
  }
}
