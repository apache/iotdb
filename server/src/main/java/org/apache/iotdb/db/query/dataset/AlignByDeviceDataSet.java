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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.IQueryRouter;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;


/**
 * This QueryDataSet is used for ALIGN_BY_DEVICE query result.
 */
public class AlignByDeviceDataSet extends QueryDataSet {

  private DataSetType dataSetType;
  private IQueryRouter queryRouter;
  private QueryContext context;
  private IExpression expression;

  private List<String> deduplicatedMeasurementColumns;
  private Map<String, Set<String>> deviceToMeasurementsMap;
  private Map<String, IExpression> deviceToFilterMap;

  // the measurements that do not exist in any device,
  // data type is considered as Boolean. The value is considered as null
  private List<String> notExistMeasurements;
  private List<Integer> positionOfNotExistMeasurements;
  // the measurements that have quotation mark. e.g. "abc",
  // '11', the data type is considered as String and the value is considered is the same with measurement name
  private List<String> constMeasurements;
  private List<Integer> positionOfConstMeasurements;

  private GroupByPlan groupByPlan;
  private FillQueryPlan fillQueryPlan;
  private AggregationPlan aggregationPlan;
  private RawDataQueryPlan rawDataQueryPlan;

  private boolean curDataSetInitialized;
  private Iterator<String> deviceIterator;
  private String currentDevice;
  private QueryDataSet currentDataSet;
  private int[] currentColumnMapRelation;
  private Map<Path, TSDataType> tsDataTypeMap;

  public AlignByDeviceDataSet(AlignByDevicePlan alignByDevicePlan, QueryContext context,
      IQueryRouter queryRouter) {
    super(null, alignByDevicePlan.getDataTypes());

    // get deduplicated measurement columns (already deduplicated in TSServiceImpl.getAlignByDeviceQueryHeaders)
    this.deduplicatedMeasurementColumns = alignByDevicePlan.getMeasurements();
    this.tsDataTypeMap = alignByDevicePlan.getDataTypeMapping();
    this.queryRouter = queryRouter;
    this.context = context;
    this.deviceToMeasurementsMap = alignByDevicePlan.getDeviceToMeasurementsMap();
    this.deviceToFilterMap = alignByDevicePlan.getDeviceToFilterMap();
    this.notExistMeasurements = alignByDevicePlan.getNotExistMeasurements();
    this.constMeasurements = alignByDevicePlan.getConstMeasurements();
    this.positionOfNotExistMeasurements = alignByDevicePlan.getPositionOfNotExistMeasurements();
    this.positionOfConstMeasurements = alignByDevicePlan.getPositionOfConstMeasurements();

    switch (alignByDevicePlan.getOperatorType()) {
      case GROUPBY:
        this.dataSetType = DataSetType.GROUPBY;
        this.groupByPlan = alignByDevicePlan.getGroupByPlan();
        break;
      case AGGREGATION:
        this.dataSetType = DataSetType.AGGREGATE;
        this.aggregationPlan = alignByDevicePlan.getAggregationPlan();
        break;
      case FILL:
        this.dataSetType = DataSetType.FILL;
        this.fillQueryPlan = alignByDevicePlan.getFillQueryPlan();
        break;
      default:
        this.dataSetType = DataSetType.QUERY;
        this.rawDataQueryPlan = new RawDataQueryPlan();
    }

    this.curDataSetInitialized = false;
    this.deviceIterator = deviceToMeasurementsMap.keySet().iterator();
    this.currentColumnMapRelation = new int[deduplicatedMeasurementColumns.size()];
  }

  protected boolean hasNextWithoutConstraint() throws IOException {
    if (curDataSetInitialized && currentDataSet.hasNext()) {
      return true;
    } else {
      curDataSetInitialized = false;
    }

    while (deviceIterator.hasNext()) {
      for (int i = 0; i < deduplicatedMeasurementColumns.size(); i++) {
        currentColumnMapRelation[i] = -1;
      }
      currentDevice = deviceIterator.next();
      Set<String> measurementColumnsOfGivenDevice = deviceToMeasurementsMap
          .get(currentDevice);

      // get columns to execute for the current device and the column map relation
      // e.g. root.sg.d0's measurementColumnsOfGivenDevice is {s2,s3}, and
      // deduplicatedMeasurementColumns is {s1,s2,s3,s4,s5},
      // then the final executeColumns is [s2,s3], currentColumnMapRelation is [-1,0,1,-1,-1].
      List<String> executeColumns = new ArrayList<>();
      int indexInExecuteColumns = -1;
      for (String column : measurementColumnsOfGivenDevice) {
        for (int i = 0; i < deduplicatedMeasurementColumns.size(); i++) {
          String columnToExecute = deduplicatedMeasurementColumns.get(i);
          if (columnToExecute.equals(column)) {
            executeColumns.add(column);
            indexInExecuteColumns++;
            currentColumnMapRelation[i] = indexInExecuteColumns;
            break;
          }
        }
      }
      // extract paths and aggregations if exist from executeColumns
      List<Path> executePaths = new ArrayList<>();
      List<TSDataType> tsDataTypes = new ArrayList<>();
      List<String> executeAggregations = new ArrayList<>();
      for (String column : executeColumns) {
        if (dataSetType == DataSetType.GROUPBY || dataSetType == DataSetType.AGGREGATE) {
          Path path = new Path(currentDevice,
              column.substring(column.indexOf('(') + 1, column.indexOf(')')));
          tsDataTypes.add(tsDataTypeMap.get(path));
          executePaths.add(path);
          executeAggregations.add(column.substring(0, column.indexOf('(')));
        } else {
          Path path = new Path(currentDevice, column);
          tsDataTypes.add(tsDataTypeMap.get(path));
          executePaths.add(path);
        }
      }

      // get filter to execute for the current device
      if (deviceToFilterMap != null) {
        this.expression = deviceToFilterMap.get(currentDevice);
      }

      try {
        switch (dataSetType) {
          case GROUPBY:
            groupByPlan.setDeduplicatedPaths(executePaths);
            groupByPlan.setDeduplicatedDataTypes(tsDataTypes);
            groupByPlan.setDeduplicatedAggregations(executeAggregations);
            currentDataSet = queryRouter.groupBy(groupByPlan, context);
            break;
          case AGGREGATE:
            aggregationPlan.setDeduplicatedPaths(executePaths);
            aggregationPlan.setDeduplicatedAggregations(executeAggregations);
            aggregationPlan.setDeduplicatedDataTypes(tsDataTypes);
            aggregationPlan.setExpression(expression);
            currentDataSet = queryRouter.aggregate(aggregationPlan, context);
            break;
          case FILL:
            fillQueryPlan.setDeduplicatedDataTypes(tsDataTypes);
            fillQueryPlan.setDeduplicatedPaths(executePaths);
            currentDataSet = queryRouter.fill(fillQueryPlan, context);
            break;
          case QUERY:
            rawDataQueryPlan.setDeduplicatedPaths(executePaths);
            rawDataQueryPlan.setDeduplicatedDataTypes(tsDataTypes);
            rawDataQueryPlan.setExpression(expression);
            currentDataSet = queryRouter.rawDataQuery(rawDataQueryPlan, context);
            break;
          default:
            throw new IOException("unsupported DataSetType");
        }
      } catch (QueryProcessException | QueryFilterOptimizationException | StorageEngineException | IOException e) {
        throw new IOException(e);
      }

      if (currentDataSet.hasNext()) {
        curDataSetInitialized = true;
        return true;
      }
    }
    return false;
  }

  protected RowRecord nextWithoutConstraint() throws IOException {
    RowRecord originRowRecord = currentDataSet.next();

    RowRecord rowRecord = new RowRecord(originRowRecord.getTimestamp());

    Field deviceField = new Field(TSDataType.TEXT);
    deviceField.setBinaryV(new Binary(currentDevice));
    rowRecord.addField(deviceField);

    List<Field> measurementFields = originRowRecord.getFields();
    for (int mapPos : currentColumnMapRelation) {
      if (mapPos == -1) {
        rowRecord.addField(null);
      } else {
        rowRecord.addField(measurementFields.get(mapPos));
      }
    }

    // build record with constant and non exist measurement
    RowRecord outRecord = new RowRecord(originRowRecord.getTimestamp());
    int loc = 0;
    int totalSize = notExistMeasurements.size() + constMeasurements.size()
        + rowRecord.getFields().size();
    int notExistMeasurementsLoc = 0;
    int constMeasurementsLoc = 0;
    int resLoc = 0;
    // don't forget device column, so loc - 1 is for looking up constant and non exist column
    while (loc < totalSize) {
      if (notExistMeasurementsLoc < notExistMeasurements.size()
          && loc - 1 == positionOfNotExistMeasurements.get(notExistMeasurementsLoc)) {
        outRecord.addField(null);
        notExistMeasurementsLoc++;
      } else if (constMeasurementsLoc < constMeasurements.size()
          && loc - 1 == positionOfConstMeasurements.get(constMeasurementsLoc)) {
        Field res = new Field(TSDataType.TEXT);
        res.setBinaryV(Binary.valueOf(constMeasurements.get(constMeasurementsLoc)));
        outRecord.addField(res);
        constMeasurementsLoc++;
      } else {
        outRecord.addField(rowRecord.getFields().get(resLoc));
        resLoc++;
      }

      loc++;
    }

    return outRecord;
  }

  private enum DataSetType {
    GROUPBY, AGGREGATE, FILL, QUERY
  }

}
