/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public class ClusterQueryRouter extends EngineQueryRouter {

  private MetaGroupMember metaGroupMember;

  ClusterQueryRouter(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
    executorFactory = expression -> new ClusterDataQueryExecutor(expression, this.metaGroupMember);
  }

  @Override
  public QueryDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, QueryContext context) {
    throw new UnsupportedOperationException("Aggregate not implemented");
  }

  @Override
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType,
      QueryContext context) {
    throw new UnsupportedOperationException("Fill not implemented");
  }
}
