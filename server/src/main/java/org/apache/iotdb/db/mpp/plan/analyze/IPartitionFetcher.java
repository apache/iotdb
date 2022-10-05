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
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;

import java.util.List;
import java.util.Map;

public interface IPartitionFetcher {

  /** get schema partition without automatically create storage group, used in query scenarios. */
  SchemaPartition getSchemaPartition(PathPatternTree patternTree);

  /**
   * get or create schema partition, used in write scenarios. if storage group not exists, then
   * automatically create.
   */
  SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree);

  /**
   * get data partition without automatically create storage group, used in query scenarios.
   *
   * @param sgNameToQueryParamsMap storage group name -> the list of DataPartitionQueryParams
   */
  DataPartition getDataPartition(Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap);

  /**
   * get or create data partition, used in write scenarios. if storage group not exists, then
   * automatically create.
   *
   * @param sgNameToQueryParamsMap storage group name -> the list of DataPartitionQueryParams
   */
  DataPartition getOrCreateDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap);

  /**
   * get or create data partition, used in write scenarios. if storage group not exists, then
   * automatically create.
   *
   * @param dataPartitionQueryParams the list of DataPartitionQueryParams
   */
  DataPartition getOrCreateDataPartition(List<DataPartitionQueryParam> dataPartitionQueryParams);

  /** get schema partition and matched nodes according to path pattern tree. */
  default SchemaNodeManagementPartition getSchemaNodeManagementPartition(
      PathPatternTree patternTree) {
    return getSchemaNodeManagementPartitionWithLevel(patternTree, null);
  }

  /** get schema partition and matched nodes according to path pattern tree and node level. */
  SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
      PathPatternTree patternTree, Integer level);

  /** update region cache in partition cache when receive request from config node */
  boolean updateRegionCache(TRegionRouteReq req);

  /** invalid all partition cache */
  void invalidAllCache();
}
