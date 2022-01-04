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
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.IQueryRouter;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** This QueryDataSet is used for ALIGN_BY_DEVICE query result. */
public class AlignByDeviceDataSet extends QueryDataSet {

  private DataSetType dataSetType;
  private IQueryRouter queryRouter;
  private QueryContext context;
  private IExpression expression;

  private List<String> measurements;
  private List<PartialPath> paths;
  private List<String> aggregations;
  private Map<String, List<Integer>> deviceToPathIndex;
  private Map<String, IExpression> deviceToFilterMap;

  private GroupByTimePlan groupByTimePlan;
  private GroupByTimeFillPlan groupByFillPlan;
  private FillQueryPlan fillQueryPlan;
  private AggregationPlan aggregationPlan;
  private RawDataQueryPlan rawDataQueryPlan;

  private boolean curDataSetInitialized;
  private QueryDataSet currentDataSet;
  private Iterator<String> deviceIterator;
  private String currentDevice;
  private List<String> executeColumns;
  private int pathsNum = 0;

  public AlignByDeviceDataSet(
      AlignByDevicePlan alignByDevicePlan, QueryContext context, IQueryRouter queryRouter) {
    super(null, null);
    // align by device's column number is different from other datasets
    // TODO I don't know whether it's right or not in AlignedPath, remember to check here while
    // adapting AlignByDevice query for new vector
    super.columnNum = alignByDevicePlan.getMeasurements().size() + 1; // + 1 for 'device'
    this.measurements = alignByDevicePlan.getMeasurements();
    this.paths = alignByDevicePlan.getDeduplicatePaths();
    this.aggregations = alignByDevicePlan.getAggregations();
    this.queryRouter = queryRouter;
    this.context = context;
    this.deviceIterator = alignByDevicePlan.getDeviceToPathIndex().keySet().iterator();
    this.deviceToPathIndex = alignByDevicePlan.getDeviceToPathIndex();
    this.deviceToFilterMap = alignByDevicePlan.getDeviceToFilterMap();

    switch (alignByDevicePlan.getOperatorType()) {
      case GROUP_BY_FILL:
        this.dataSetType = DataSetType.GROUP_BY_FILL;
        this.groupByFillPlan = alignByDevicePlan.getGroupByFillPlan();
        this.groupByFillPlan.setAscending(alignByDevicePlan.isAscending());
        break;
      case GROUP_BY_TIME:
        this.dataSetType = DataSetType.GROUP_BY_TIME;
        this.groupByTimePlan = alignByDevicePlan.getGroupByTimePlan();
        this.groupByTimePlan.setAscending(alignByDevicePlan.isAscending());
        break;
      case AGGREGATION:
        this.dataSetType = DataSetType.AGGREGATE;
        this.aggregationPlan = alignByDevicePlan.getAggregationPlan();
        this.aggregationPlan.setAscending(alignByDevicePlan.isAscending());
        break;
      case FILL:
        this.dataSetType = DataSetType.FILL;
        this.fillQueryPlan = alignByDevicePlan.getFillQueryPlan();
        this.fillQueryPlan.setAscending(alignByDevicePlan.isAscending());
        break;
      default:
        this.dataSetType = DataSetType.QUERY;
        this.rawDataQueryPlan = new RawDataQueryPlan();
        this.rawDataQueryPlan.setAscending(alignByDevicePlan.isAscending());
        // only redirect query for raw data query
        this.rawDataQueryPlan.setEnableRedirect(alignByDevicePlan.isEnableRedirect());
    }

    this.curDataSetInitialized = false;
  }

  public int getPathsNum() {
    return pathsNum;
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public boolean hasNextWithoutConstraint() throws IOException {
    if (curDataSetInitialized && currentDataSet.hasNext()) {
      return true;
    } else {
      curDataSetInitialized = false;
    }

    while (deviceIterator.hasNext()) {
      currentDevice = deviceIterator.next();
      executeColumns = new ArrayList<>();
      List<PartialPath> executePaths = new ArrayList<>();
      List<String> executeAggregations = new ArrayList<>();
      for (int i : deviceToPathIndex.get(currentDevice)) {
        executePaths.add(paths.get(i));
        String executeColumn = paths.get(i).getMeasurement();
        if (aggregations != null) {
          executeAggregations.add(aggregations.get(i));
          executeColumn = String.format("%s(%s)", aggregations.get(i), executeColumn);
        }
        executeColumns.add(executeColumn);
      }

      // get filter to execute for the current device
      if (deviceToFilterMap != null) {
        this.expression = deviceToFilterMap.get(currentDevice);
      }

      // for tracing: try to calculate the number of series paths
      if (context.isEnableTracing()) {
        pathsNum += executeColumns.size();
      }

      try {
        switch (dataSetType) {
          case GROUP_BY_FILL:
            groupByFillPlan.setDeduplicatedPathsAndUpdate(executePaths);
            groupByFillPlan.setDeduplicatedAggregations(executeAggregations);
            groupByFillPlan.setExpression(expression);
            currentDataSet = queryRouter.groupByFill(groupByFillPlan, context);
            break;
          case GROUP_BY_TIME:
            groupByTimePlan.setDeduplicatedPathsAndUpdate(executePaths);
            groupByTimePlan.setDeduplicatedAggregations(executeAggregations);
            groupByTimePlan.setExpression(expression);
            currentDataSet = queryRouter.groupBy(groupByTimePlan, context);
            break;
          case AGGREGATE:
            aggregationPlan.setDeduplicatedPathsAndUpdate(executePaths);
            aggregationPlan.setDeduplicatedAggregations(executeAggregations);
            aggregationPlan.setExpression(expression);
            currentDataSet = queryRouter.aggregate(aggregationPlan, context);
            break;
          case FILL:
            fillQueryPlan.setDeduplicatedPathsAndUpdate(executePaths);
            currentDataSet = queryRouter.fill(fillQueryPlan, context);
            break;
          case QUERY:
            // Group all the subSensors of one vector into one VectorPartialPath
            executePaths = MetaUtils.groupAlignedPaths(executePaths);
            rawDataQueryPlan.setDeduplicatedPathsAndUpdate(executePaths);
            rawDataQueryPlan.setExpression(expression);
            currentDataSet = queryRouter.rawDataQuery(rawDataQueryPlan, context);
            break;
          default:
            throw new IOException("unsupported DataSetType");
        }
      } catch (QueryProcessException
          | QueryFilterOptimizationException
          | StorageEngineException e) {
        throw new IOException(e);
      }

      if (currentDataSet.getEndPoint() != null) {
        org.apache.iotdb.service.rpc.thrift.EndPoint endPoint =
            new org.apache.iotdb.service.rpc.thrift.EndPoint();
        endPoint.setIp(currentDataSet.getEndPoint().getIp());
        endPoint.setPort(currentDataSet.getEndPoint().getPort());
        throw new RedirectException(endPoint);
      }

      if (currentDataSet.hasNext()) {
        curDataSetInitialized = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    RowRecord originRowRecord = currentDataSet.next();

    RowRecord rowRecord = new RowRecord(originRowRecord.getTimestamp());

    Field deviceField = new Field(TSDataType.TEXT);
    deviceField.setBinaryV(new Binary(currentDevice));
    rowRecord.addField(deviceField);
    // device field should not be considered as a value field it should affect the WITHOUT NULL
    // judgement
    rowRecord.resetNullFlag();

    List<Field> measurementFields = originRowRecord.getFields();
    Map<String, Field> currentColumnMap = new HashMap<>();
    for (int i = 0; i < measurementFields.size(); i++) {
      currentColumnMap.put(executeColumns.get(i), measurementFields.get(i));
    }

    for (String measurement : measurements) {
      if (currentColumnMap.get(measurement) != null) {
        rowRecord.addField(currentColumnMap.get(measurement));
      } else {
        rowRecord.addField(new Field(null));
      }
    }

    return rowRecord;
  }

  private enum DataSetType {
    GROUP_BY_FILL,
    GROUP_BY_TIME,
    AGGREGATE,
    FILL,
    QUERY
  }
}
