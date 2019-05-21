/**
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
package org.apache.iotdb.cluster.query.timegenerator;

import static org.apache.iotdb.tsfile.read.expression.ExpressionType.SERIES;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.FilterSeriesGroupEntity;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterFilterSeriesReader;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.timegenerator.AbstractNodeConstructor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;

public class ClusterNodeConstructor extends AbstractNodeConstructor {

  /**
   * Single query manager
   */
  private ClusterRpcSingleQueryManager queryManager;

  /**
   * Filter series reader group by  group id
   */
  private Map<String, List<ClusterFilterSeriesReader>> filterSeriesReadersByGroupId;

  /**
   * Mark filter series reader index group by group id
   */
  private Map<String, Integer> filterSeriesReaderIndex;

  public ClusterNodeConstructor(ClusterRpcSingleQueryManager queryManager) {
    this.queryManager = queryManager;
    this.filterSeriesReadersByGroupId = new HashMap<>();
    this.filterSeriesReaderIndex = new HashMap<>();
    this.init(queryManager);
  }

  /**
   * Init filter series reader
   */
  private void init(ClusterRpcSingleQueryManager queryManager) {
    Map<String, FilterSeriesGroupEntity> filterGroupEntityMap = queryManager.getFilterSeriesGroupEntityMap();
    filterGroupEntityMap.forEach(
        (key, value) -> filterSeriesReadersByGroupId.put(key, value.getFilterSeriesReaders()));
    filterSeriesReadersByGroupId.forEach((key, value) -> filterSeriesReaderIndex.put(key, 0));
  }

  /**
   * Construct expression node.
   *
   * @param expression expression
   * @return Node object
   * @throws IOException IOException
   * @throws FileNodeManagerException FileNodeManagerException
   */
  @Override
  public Node construct(IExpression expression, QueryContext context)
      throws FileNodeManagerException {
    if (expression.getType() == SERIES) {
      try {
        Path seriesPath = ((SingleSeriesExpression) expression).getSeriesPath();
        String groupId = QPExecutorUtils.getGroupIdByDevice(seriesPath.getDevice());
        if (filterSeriesReadersByGroupId.containsKey(groupId)) {
          List<ClusterFilterSeriesReader> seriesReaders = filterSeriesReadersByGroupId.get(groupId);
          int readerIndex = filterSeriesReaderIndex.get(groupId);
          filterSeriesReaderIndex.put(groupId, readerIndex + 1);
          return new ClusterLeafNode(seriesReaders.get(readerIndex));
        } else {
          queryManager.addDataGroupUsage(groupId);
          return new ClusterLeafNode(generateSeriesReader((SingleSeriesExpression) expression,
              context));
        }
      } catch (IOException | PathErrorException e) {
        throw new FileNodeManagerException(e);
      }
    } else {
      return constructNotSeriesNode(expression, context);
    }
  }
}
