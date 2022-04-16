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
package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.execution.Coordinator;
import org.apache.iotdb.db.mpp.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SchemaFetchStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.Binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterSchemaFetcher implements ISchemaFetcher {

  private final Coordinator coordinator = Coordinator.getInstance();
  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();

  private static final class ClusterSchemaFetcherHolder {
    private static final ClusterSchemaFetcher INSTANCE = new ClusterSchemaFetcher();

    private ClusterSchemaFetcherHolder() {}
  }

  public static ClusterSchemaFetcher getInstance() {
    return ClusterSchemaFetcherHolder.INSTANCE;
  }

  private ClusterSchemaFetcher() {}

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree) {
    SchemaPartition schemaPartition = partitionFetcher.getSchemaPartition(patternTree);
    Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap =
        schemaPartition.getSchemaPartitionMap();
    List<String> storageGroups = new ArrayList<>(schemaPartitionMap.keySet());

    SchemaFetchStatement schemaFetchStatement = new SchemaFetchStatement(patternTree);
    schemaFetchStatement.setSchemaPartition(schemaPartition);

    QueryId queryId =
        new QueryId(String.valueOf(SessionManager.getInstance().requestQueryId(false)));
    ExecutionResult executionResult =
        coordinator.execute(schemaFetchStatement, queryId, null, "", partitionFetcher, this);
    // TODO: (xingtanzjr) throw exception
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException("cannot fetch schema, status is: " + executionResult.status);
    }
    TsBlock tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
    // TODO: (xingtanzjr) need to release this query's resource here
    SchemaTree result = new SchemaTree();
    result.setStorageGroups(storageGroups);
    Binary binary;
    SchemaTree fetchedSchemaTree;
    Column column = tsBlock.getColumn(0);
    for (int i = 0; i < column.getPositionCount(); i++) {
      binary = column.getBinary(i);
      fetchedSchemaTree = SchemaTree.deserialize(ByteBuffer.wrap(binary.getValues()));
      result.mergeSchemaTree(fetchedSchemaTree);
    }
    return result;
  }

  @Override
  public SchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean aligned) {
    // todo implement auto create schema
    return fetchSchema(new PathPatternTree(devicePath, measurements));
  }

  @Override
  public SchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePath,
      List<String[]> measurements,
      List<TSDataType[]> tsDataTypes,
      List<Boolean> aligned) {
    Map<PartialPath, List<String>> deviceToMeasurementMap = new HashMap<>();
    for (int i = 0; i < devicePath.size(); i++) {
      deviceToMeasurementMap.put(devicePath.get(i), Arrays.asList(measurements.get(i)));
    }
    // todo implement auto create schema
    return fetchSchema(new PathPatternTree(deviceToMeasurementMap));
  }
}
