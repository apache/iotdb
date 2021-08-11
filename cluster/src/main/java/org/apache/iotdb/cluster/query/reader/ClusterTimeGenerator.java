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

package org.apache.iotdb.cluster.query.reader;

import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.LeafNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.List;

public class ClusterTimeGenerator extends ServerTimeGenerator {

  private ClusterReaderFactory readerFactory;
  private boolean hasLocalReader = false;
  private QueryDataSet.EndPoint endPoint = null;

  /** Constructor of EngineTimeGenerator. */
  public ClusterTimeGenerator(
      QueryContext context,
      MetaGroupMember metaGroupMember,
      RawDataQueryPlan rawDataQueryPlan,
      boolean onlyCheckLocalData)
      throws StorageEngineException {
    super(context);
    this.queryPlan = rawDataQueryPlan;
    this.readerFactory = new ClusterReaderFactory(metaGroupMember);
    try {
      readerFactory.syncMetaGroup();
      if (onlyCheckLocalData) {
        whetherHasLocalDataGroup(
            queryPlan.getExpression(), metaGroupMember, queryPlan.isAscending());
      } else {
        constructNode(queryPlan.getExpression());
      }
    } catch (IOException | CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
  }

  @TestOnly
  public ClusterTimeGenerator(
      QueryContext context,
      MetaGroupMember metaGroupMember,
      ClusterReaderFactory clusterReaderFactory,
      RawDataQueryPlan rawDataQueryPlan,
      boolean onlyCheckLocalData)
      throws StorageEngineException {
    super(context);
    this.queryPlan = rawDataQueryPlan;
    this.readerFactory = clusterReaderFactory;
    try {
      readerFactory.syncMetaGroup();
      if (onlyCheckLocalData) {
        whetherHasLocalDataGroup(
            queryPlan.getExpression(), metaGroupMember, queryPlan.isAscending());
      } else {
        constructNode(queryPlan.getExpression());
      }
    } catch (IOException | CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
  }

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {
    Filter filter = expression.getFilter();
    Filter timeFilter = getTimeFilter(filter);
    PartialPath path = (PartialPath) expression.getSeriesPath();
    TSDataType dataType;
    ManagedSeriesReader mergeReader;
    try {
      dataType = IoTDB.metaManager.getSeriesType(path);
      mergeReader =
          readerFactory.getSeriesReader(
              path,
              queryPlan.getAllMeasurementsInDevice(path.getDevice()),
              dataType,
              timeFilter,
              filter,
              context,
              queryPlan.isAscending());
    } catch (Exception e) {
      throw new IOException(e);
    }
    return mergeReader;
  }

  public boolean isHasLocalReader() {
    return hasLocalReader;
  }

  @Override
  public String toString() {
    return super.toString() + ", has local reader:" + hasLocalReader;
  }

  public void whetherHasLocalDataGroup(
      IExpression expression, MetaGroupMember metaGroupMember, boolean isAscending)
      throws IOException {
    this.hasLocalReader = false;
    constructNode(expression, metaGroupMember, isAscending);
  }

  private Node constructNode(
      IExpression expression, MetaGroupMember metaGroupMember, boolean isAscending)
      throws IOException {
    if (expression.getType() == ExpressionType.SERIES) {
      SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) expression;
      checkHasLocalReader(singleSeriesExp, metaGroupMember);
      return new LeafNode(null);
    } else {
      Node leftChild =
          constructNode(((IBinaryExpression) expression).getLeft(), metaGroupMember, isAscending);
      Node rightChild =
          constructNode(((IBinaryExpression) expression).getRight(), metaGroupMember, isAscending);

      if (expression.getType() == ExpressionType.OR) {
        return new OrNode(leftChild, rightChild, isAscending);
      } else if (expression.getType() == ExpressionType.AND) {
        return new AndNode(leftChild, rightChild, isAscending);
      }
      throw new UnSupportedDataTypeException(
          "Unsupported ExpressionType when construct OperatorNode: " + expression.getType());
    }
  }

  private void checkHasLocalReader(
      SingleSeriesExpression expression, MetaGroupMember metaGroupMember) throws IOException {
    Filter filter = expression.getFilter();
    Filter timeFilter = getTimeFilter(filter);
    PartialPath path = (PartialPath) expression.getSeriesPath();
    TSDataType dataType;
    try {
      dataType = IoTDB.metaManager.getSeriesType(path);

      List<PartitionGroup> partitionGroups = metaGroupMember.routeFilter(null, path);
      for (PartitionGroup partitionGroup : partitionGroups) {
        if (partitionGroup.contains(metaGroupMember.getThisNode())) {
          DataGroupMember dataGroupMember =
              metaGroupMember.getLocalDataMember(
                  partitionGroup.getHeader(),
                  String.format(
                      "Query: %s, time filter: %s, queryId: %d", path, null, context.getQueryId()));

          IPointReader pointReader =
              readerFactory.getSeriesPointReader(
                  path,
                  queryPlan.getAllMeasurementsInDevice(path.getDevice()),
                  dataType,
                  timeFilter,
                  filter,
                  context,
                  dataGroupMember,
                  queryPlan.isAscending(),
                  null);

          if (pointReader.hasNextTimeValuePair()) {
            this.hasLocalReader = true;
            this.endPoint = null;
            pointReader.close();
            break;
          }
          pointReader.close();
        } else if (endPoint == null) {
          endPoint =
              new QueryDataSet.EndPoint(
                  partitionGroup.getHeader().getNode().getClientIp(),
                  partitionGroup.getHeader().getNode().getClientPort());
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
