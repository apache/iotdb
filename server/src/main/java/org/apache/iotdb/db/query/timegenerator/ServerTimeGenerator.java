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
package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A timestamp generator for query with filter. e.g. For query clause "select s1, s2 from root where
 * s3 < 0 and time > 100", this class can iterate back to every timestamp of the query.
 */
public class ServerTimeGenerator extends TimeGenerator {

  protected QueryContext context;
  protected RawDataQueryPlan queryPlan;

  public ServerTimeGenerator(QueryContext context) {
    this.context = context;
  }

  /** Constructor of EngineTimeGenerator. */
  public ServerTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    this.context = context;
    this.queryPlan = queryPlan;
    try {
      serverConstructNode(queryPlan.getExpression());
    } catch (IOException e) {
      throw new StorageEngineException(e);
    }
  }

  public void serverConstructNode(IExpression expression)
      throws IOException, StorageEngineException {
    List<PartialPath> pathList = new ArrayList<>();
    getPartialPathFromExpression(expression, pathList);
    List<StorageGroupProcessor> list = StorageEngine.getInstance().mergeLock(pathList);
    try {
      operatorNode = construct(expression);
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  private void getPartialPathFromExpression(IExpression expression, List<PartialPath> pathList) {
    if (expression.getType() == ExpressionType.SERIES) {
      pathList.add((PartialPath) ((SingleSeriesExpression) expression).getSeriesPath());
    } else {
      getPartialPathFromExpression(((IBinaryExpression) expression).getLeft(), pathList);
      getPartialPathFromExpression(((IBinaryExpression) expression).getRight(), pathList);
    }
  }

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {
    Filter valueFilter = expression.getFilter();
    PartialPath path = (PartialPath) expression.getSeriesPath();
    TSDataType dataType;
    QueryDataSource queryDataSource;
    try {
      dataType = IoTDB.metaManager.getSeriesType(path);
      queryDataSource =
          QueryResourceManager.getInstance().getQueryDataSource(path, context, valueFilter);
      // update valueFilter by TTL
      valueFilter = queryDataSource.updateFilterUsingTTL(valueFilter);
    } catch (Exception e) {
      throw new IOException(e);
    }

    // get the TimeFilter part in SingleSeriesExpression
    Filter timeFilter = getTimeFilter(valueFilter);

    return new SeriesRawDataBatchReader(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        queryDataSource,
        timeFilter,
        valueFilter,
        null,
        queryPlan.isAscending());
  }

  /** extract time filter from a value filter */
  protected Filter getTimeFilter(Filter filter) {
    if (filter instanceof UnaryFilter
        && ((UnaryFilter) filter).getFilterType() == FilterType.TIME_FILTER) {
      return filter;
    }
    if (filter instanceof AndFilter) {
      Filter leftTimeFilter = getTimeFilter(((AndFilter) filter).getLeft());
      Filter rightTimeFilter = getTimeFilter(((AndFilter) filter).getRight());
      if (leftTimeFilter != null && rightTimeFilter != null) {
        return filter;
      } else if (leftTimeFilter != null) {
        return leftTimeFilter;
      } else {
        return rightTimeFilter;
      }
    }
    return null;
  }

  @Override
  protected boolean isAscending() {
    return queryPlan.isAscending();
  }
}
