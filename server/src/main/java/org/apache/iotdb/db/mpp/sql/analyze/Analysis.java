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

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Analysis used for planning a query. TODO: This class may need to store more info for a query. */
public class Analysis {
  // Description for each series. Such as dataType, existence

  // Data distribution info for each series. Series -> [DataRegion, DataRegion]

  // Map<PartialPath, List<FullPath>> Used to remove asterisk

  // Statement
  private Statement statement;

  // indicate whether this statement is write or read
  private QueryType queryType;

  private DataPartition dataPartition;

  private SchemaPartition schemaPartition;

  private SchemaTree schemaTree;

  private Map<String, Set<PartialPath>> deviceIdToPathsMap;

  public List<RegionReplicaSet> getPartitionInfo(PartialPath seriesPath, Filter timefilter) {
    // TODO: (xingtanzjr) implement the calculation of timePartitionIdList
    return dataPartition.getDataRegionReplicaSet(seriesPath.getDevice(), null);
  }

  public Statement getStatement() {
    return statement;
  }

  public void setStatement(Statement statement) {
    this.statement = statement;
  }

  public DataPartition getDataPartitionInfo() {
    return dataPartition;
  }

  public void setDataPartitionInfo(DataPartition dataPartition) {
    this.dataPartition = dataPartition;
  }

  public SchemaPartition getSchemaPartitionInfo() {
    return schemaPartition;
  }

  public void setSchemaPartitionInfo(SchemaPartition schemaPartition) {
    this.schemaPartition = schemaPartition;
  }

  public Map<String, Set<PartialPath>> getDeviceIdToPathsMap() {
    return deviceIdToPathsMap;
  }

  public void setDeviceIdToPathsMap(Map<String, Set<PartialPath>> deviceIdToPathsMap) {
    this.deviceIdToPathsMap = deviceIdToPathsMap;
  }

  public SchemaTree getSchemaTree() {
    return schemaTree;
  }

  public void setSchemaTree(SchemaTree schemaTree) {
    this.schemaTree = schemaTree;
  }
}
