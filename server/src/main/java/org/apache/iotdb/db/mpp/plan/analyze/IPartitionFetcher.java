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
package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

import java.util.List;
import java.util.Map;

public interface IPartitionFetcher {

  SchemaPartition getSchemaPartition(PathPatternTree patternTree);

  SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree);

  default SchemaNodeManagementPartition getSchemaNodeManagementPartition(
      PathPatternTree patternTree) {
    return getSchemaNodeManagementPartitionWithLevel(patternTree, null);
  }

  SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
      PathPatternTree patternTree, Integer level);

  DataPartition getDataPartition(Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap);

  DataPartition getDataPartition(List<DataPartitionQueryParam> dataPartitionQueryParams);

  DataPartition getOrCreateDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap);

  DataPartition getOrCreateDataPartition(List<DataPartitionQueryParam> dataPartitionQueryParams);

  void invalidAllCache();
}
