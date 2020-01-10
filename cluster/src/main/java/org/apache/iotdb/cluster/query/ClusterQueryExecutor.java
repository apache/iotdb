/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClusterQueryExecutor extends QueryProcessExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterQueryExecutor.class);
  private MetaGroupMember metaGroupMember;

  ClusterQueryExecutor(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
    this.queryRouter = new ClusterQueryRouter(metaGroupMember);
  }

  @Override
  public TSDataType getSeriesType(Path path) throws MetadataException {
    return metaGroupMember.getSeriesType(path.getFullPath());
  }

  @Override
  public QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException, QueryFilterOptimizationException, QueryProcessException {
    if (queryPlan instanceof QueryPlan) {
      return processDataQuery((QueryPlan) queryPlan, context);
    } else {
      //TODO-Cluster: support more queries
      throw new QueryProcessException(String.format("Unrecognized query plan %s", queryPlan));
    }
  }

  @Override
  public List<String> getAllMatchedPaths(String originPath) throws MetadataException {
    if (!originPath.contains(PATH_WILDCARD)) {
      // path without wildcards does not need to be processed
      return Collections.singletonList(originPath);
    }
    // make sure this node knows all storage groups
    metaGroupMember.syncLeader();
    // get all storage groups this path may belong to
    Map<String, String> sgPathMap = MManager.getInstance().determineStorageGroup(originPath);
    List<String> ret = new ArrayList<>();
    for (Entry<String, String> entry : sgPathMap.entrySet()) {
      String storageGroupName = entry.getKey();
      String fullPath = entry.getValue();
      ret.addAll(metaGroupMember.getMatchedPaths(storageGroupName, fullPath));
    }
    logger.debug("The paths of path {} are {}", originPath, ret);

    return ret;
  }

  @Override
  public boolean judgePathExists(Path path) {
    // this is guaranteed by ConcatPathOptimizer
    return true;
  }
}
