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

package org.apache.iotdb.confignode.consensus.response.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaPartitionResp implements DataSet {

  private TSStatus status;

  private final boolean allPartitionsExist;

  // Map<StorageGroup, SchemaPartitionTable>
  // TODO: Replace this map with new SchemaPartition
  private final Map<String, SchemaPartitionTable> schemaPartition;

  public SchemaPartitionResp(
      TSStatus status,
      boolean allPartitionsExist,
      Map<String, SchemaPartitionTable> schemaPartition) {
    this.status = status;
    this.allPartitionsExist = allPartitionsExist;
    this.schemaPartition = schemaPartition;
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public boolean isAllPartitionsExist() {
    return allPartitionsExist;
  }

  public TSchemaPartitionTableResp convertToRpcSchemaPartitionTableResp() {
    TSchemaPartitionTableResp resp = new TSchemaPartitionTableResp();
    resp.setStatus(status);

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionMap =
          new ConcurrentHashMap<>();

      schemaPartition.forEach(
          (storageGroup, schemaPartitionTable) ->
              schemaPartitionMap.put(storageGroup, schemaPartitionTable.getSchemaPartitionMap()));

      resp.setSchemaPartitionTable(schemaPartitionMap);
    }

    return resp;
  }
}
