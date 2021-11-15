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
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.MeasurementInfo;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.IQueryRouter;
import org.apache.iotdb.db.service.IoTDB;
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
import java.util.Map.Entry;
import java.util.Set;

/** This QueryDataSet is used for ALIGN_BY_DEVICE query result. */
public class AlignByDeviceDataSet extends QueryDataSet {

  private DataSetType dataSetType;
  private IQueryRouter queryRouter;
  private QueryContext context;
  private IExpression expression;

  private List<String> measurements;
  private List<PartialPath> devices;
  private Map<String, IExpression> deviceToFilterMap;
  private Map<String, MeasurementInfo> measurementInfoMap;

  private GroupByTimePlan groupByTimePlan;
  private FillQueryPlan fillQueryPlan;
  private AggregationPlan aggregationPlan;
  private RawDataQueryPlan rawDataQueryPlan;

  private boolean curDataSetInitialized;
  private PartialPath currentDevice;
  private QueryDataSet currentDataSet;
  private Iterator<PartialPath> deviceIterator;
  private List<String> executeColumns;
  private int pathsNum = 0;

  public AlignByDeviceDataSet(
      AlignByDevicePlan alignByDevicePlan, QueryContext context, IQueryRouter queryRouter) {
    super(null, alignByDevicePlan.getDataTypes());
    // align by device's column number is different from other datasets
    // TODO I don't know whether it's right or not in AlignedPath, remember to check here while
    // adapting AlignByDevice query for new vector
    super.columnNum = alignByDevicePlan.getDataTypes().size();
    this.measurements = alignByDevicePlan.getMeasurements();
    this.devices = alignByDevicePlan.getDevices();
    this.measurementInfoMap = alignByDevicePlan.getMeasurementInfoMap();
    this.queryRouter = queryRouter;
    this.context = context;
    this.deviceToFilterMap = alignByDevicePlan.getDeviceToFilterMap();

    switch (alignByDevicePlan.getOperatorType()) {
      case GROUP_BY_TIME:
        this.dataSetType = DataSetType.GROUPBYTIME;
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
    this.deviceIterator = devices.iterator();
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
      // get all measurements of current device
      Map<String, MeasurementPath> measurementToPathMap =
          getMeasurementsUnderGivenDevice(currentDevice);
      Set<String> measurementOfGivenDevice = measurementToPathMap.keySet();

      // extract paths and aggregations queried from all measurements
      // executeColumns is for calculating rowRecord
      executeColumns = new ArrayList<>();
      List<PartialPath> executePaths = new ArrayList<>();
      List<String> executeAggregations = new ArrayList<>();
      for (Entry<String, MeasurementInfo> entry : measurementInfoMap.entrySet()) {
        if (entry.getValue().getMeasurementType() != MeasurementType.Exist) {
          continue;
        }
        String column = entry.getKey();
        String measurement = column;
        if (dataSetType == DataSetType.GROUPBYTIME || dataSetType == DataSetType.AGGREGATE) {
          measurement = column.substring(column.indexOf('(') + 1, column.indexOf(')'));
          if (measurementOfGivenDevice.contains(measurement)) {
            executeAggregations.add(column.substring(0, column.indexOf('(')));
          }
        }
        if (measurementOfGivenDevice.contains(measurement)) {
          executeColumns.add(column);
          executePaths.add(measurementToPathMap.get(measurement));
        }
      }

      // get filter to execute for the current device
      if (deviceToFilterMap != null) {
        this.expression = deviceToFilterMap.get(currentDevice.getFullPath());
      }

      // for tracing: try to calculate the number of series paths
      if (context.isEnableTracing()) {
        pathsNum += executeColumns.size();
      }

      try {
        switch (dataSetType) {
          case GROUPBYTIME:
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

  /** Get all measurements under given device. */
  protected Map<String, MeasurementPath> getMeasurementsUnderGivenDevice(PartialPath device)
      throws IOException {
    try {
      // TODO: Implement this method in Cluster MManager
      Map<String, MeasurementPath> measurementToPathMap = new HashMap<>();
      List<MeasurementPath> measurementPaths =
          IoTDB.metaManager.getAllMeasurementByDevicePath(device);
      for (MeasurementPath measurementPath : measurementPaths) {
        measurementToPathMap.put(measurementPath.getMeasurement(), measurementPath);
      }
      return measurementToPathMap;
    } catch (MetadataException e) {
      throw new IOException("Cannot get node from " + device, e);
    }
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    RowRecord originRowRecord = currentDataSet.next();

    RowRecord rowRecord = new RowRecord(originRowRecord.getTimestamp());

    Field deviceField = new Field(TSDataType.TEXT);
    deviceField.setBinaryV(new Binary(currentDevice.getFullPath()));
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
      MeasurementInfo measurementInfo = measurementInfoMap.get(measurement);
      switch (measurementInfo.getMeasurementType()) {
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
    GROUPBYTIME,
    AGGREGATE,
    FILL,
    QUERY
  }
}
