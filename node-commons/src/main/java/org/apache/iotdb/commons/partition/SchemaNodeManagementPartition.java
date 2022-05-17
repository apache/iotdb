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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;

import java.util.List;
import java.util.Map;

public class SchemaNodeManagementPartition {
  SchemaPartition schemaPartition;

  List<String> matchedNode;

  public SchemaNodeManagementPartition(
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap,
      String seriesSlotExecutorName,
      int seriesPartitionSlotNum,
      List<String> matchedNode) {
    this.schemaPartition =
        new SchemaPartition(schemaPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
    this.matchedNode = matchedNode;
  }

  public SchemaPartition getSchemaPartition() {
    return schemaPartition;
  }

  public void setSchemaPartition(SchemaPartition schemaPartition) {
    this.schemaPartition = schemaPartition;
  }

  public List<String> getMatchedNode() {
    return matchedNode;
  }

  public void setMatchedNode(List<String> matchedNode) {
    this.matchedNode = matchedNode;
  }
}
