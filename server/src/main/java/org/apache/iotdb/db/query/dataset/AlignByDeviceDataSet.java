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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
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

  private List<String> measurements;
  private List<String> devices;
  private Map<String, IExpression> deviceToFilterMap;
  private Map<String, MeasurementType> measurementTypeMap;
  // record the real type of the corresponding measurement
  private Map<String, TSDataType> measurementDataTypeMap;

  private GroupByTimePlan groupByTimePlan;
  private FillQueryPlan fillQueryPlan;
  private AggregationPlan aggregationPlan;
  private RawDataQueryPlan rawDataQueryPlan;

  private boolean curDataSetInitialized;
  private String currentDevice;
  private QueryDataSet currentDataSet;
  private Iterator<String> deviceIterator;
  private List<String> executeColumns;

  public AlignByDeviceDataSet(AlignByDevicePlan alignByDevicePlan, QueryContext context,
      IQueryRouter queryRouter) throws MetadataException {
    super(null, alignByDevicePlan.getDataTypes());

    this.measurements = alignByDevicePlan.getMeasurements();
    this.devices = alignByDevicePlan.getDevices();
    this.measurementDataTypeMap = alignByDevicePlan.getMeasurementDataTypeMap();
    this.queryRouter = queryRouter;
    this.context = context;
    this.deviceToFilterMap = alignByDevicePlan.getDeviceToFilterMap();
    this.measurementTypeMap = alignByDevicePlan.getMeasurementTypeMap();

    switch (alignByDevicePlan.getOperatorType()) {
      case GROUPBYTIME:
        this.dataSetType = DataSetType.GROUPBYTIME;
        this.groupByTimePlan = alignByDevicePlan.getGroupByTimePlan();
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
    this.deviceIterator = devices.iterator();
  }

  protected boolean hasNextWithoutConstraint() throws IOException {
    if (curDataSetInitialized && currentDataSet.hasNext()) {
      return true;
    } else {
      curDataSetInitialized = false;
    }

    while (deviceIterator.hasNext()) {
      currentDevice = deviceIterator.next();
      // get all measurements of current device
      Set<String> measurementOfGivenDevice = getDeviceMeasurements(currentDevice);

      // extract paths and aggregations queried from all measurements
      // executeColumns is for calculating rowRecord
      executeColumns = new ArrayList<>();
      List<Path> executePaths = new ArrayList<>();
      List<TSDataType> tsDataTypes = new ArrayList<>();
      List<String> executeAggregations = new ArrayList<>();
      for (String column : measurementDataTypeMap.keySet()) {
        String measurement = column;
        if (dataSetType == DataSetType.GROUPBYTIME || dataSetType == DataSetType.AGGREGATE) {
          measurement = column.substring(column.indexOf('(') + 1, column.indexOf(')'));
          if (measurementOfGivenDevice.contains(measurement)) {
            executeAggregations.add(column.substring(0, column.indexOf('(')));
          }
        }
        if (measurementOfGivenDevice.contains(measurement)) {
          executeColumns.add(column);
          executePaths.add(new Path(currentDevice, measurement));
          tsDataTypes.add(measurementDataTypeMap.get(column));
        }
      }

      // get filter to execute for the current device
      if (deviceToFilterMap != null) {
        this.expression = deviceToFilterMap.get(currentDevice);
      }

      try {
        switch (dataSetType) {
          case GROUPBYTIME:
            groupByTimePlan.setDeduplicatedPaths(executePaths);
            groupByTimePlan.setDeduplicatedDataTypes(tsDataTypes);
            groupByTimePlan.setDeduplicatedAggregations(executeAggregations);
            currentDataSet = queryRouter.groupBy(groupByTimePlan, context);
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
      } catch (QueryProcessException | QueryFilterOptimizationException | StorageEngineException e) {
        throw new IOException(e);
      }

      if (currentDataSet.hasNext()) {
        curDataSetInitialized = true;
        return true;
      }
    }
    return false;
  }

  protected Set<String> getDeviceMeasurements(String device) throws IOException {
    try {
      MNode deviceNode = MManager.getInstance().getNodeByPath(device);
      return deviceNode.getChildren().keySet();
    } catch (MetadataException e) {
      throw new IOException("Cannot get node from " + device, e);
    }
  }

  protected RowRecord nextWithoutConstraint() throws IOException {
    RowRecord originRowRecord = currentDataSet.next();

    RowRecord rowRecord = new RowRecord(originRowRecord.getTimestamp());

    Field deviceField = new Field(TSDataType.TEXT);
    deviceField.setBinaryV(new Binary(currentDevice));
    rowRecord.addField(deviceField);

    List<Field> measurementFields = originRowRecord.getFields();
    Map<String, Field> currentColumnMap = new HashMap<>();
    for (int i = 0; i < measurementFields.size(); i++) {
      currentColumnMap.put(executeColumns.get(i), measurementFields.get(i));
    }

    for (String measurement : measurements) {
      switch (measurementTypeMap.get(measurement)) {
        case Exist:
          if (currentColumnMap.get(measurement) != null) {
            rowRecord.addField(currentColumnMap.get(measurement));
          } else {
            rowRecord.addField(new Field(null));
          }
          break;
        case NonExist:
          rowRecord.addField(new Field(null));
          break;
        case Constant:
          Field res = new Field(TSDataType.TEXT);
          res.setBinaryV(Binary.valueOf(measurement));
          rowRecord.addField(res);
          break;
      }
    }

    return rowRecord;
  }

  private enum DataSetType {
    GROUPBYTIME, AGGREGATE, FILL, QUERY
  }

}
