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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
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
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
      Set<String> measurementOfGivenDevice = getMeasurementsUnderGivenDevice(currentDevice);

      // extract paths and aggregations queried from all measurements
      // executeColumns is for calculating rowRecord
      executeColumns = new ArrayList<>();
      List<PartialPath> executePaths = new ArrayList<>();
      List<TSDataType> tsDataTypes = new ArrayList<>();
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
          executePaths.add(transformPath(currentDevice, measurement));
          tsDataTypes.add(measurementInfoMap.get(column).getMeasurementDataType());
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
            groupByTimePlan.setDeduplicatedDataTypes(tsDataTypes);
            groupByTimePlan.setDeduplicatedAggregations(executeAggregations);
            groupByTimePlan.setExpression(expression);
            currentDataSet = queryRouter.groupBy(groupByTimePlan, context);
            break;
          case AGGREGATE:
            aggregationPlan.setDeduplicatedPathsAndUpdate(executePaths);
            aggregationPlan.setDeduplicatedAggregations(executeAggregations);
            aggregationPlan.setDeduplicatedDataTypes(tsDataTypes);
            aggregationPlan.setExpression(expression);
            currentDataSet = queryRouter.aggregate(aggregationPlan, context);
            break;
          case FILL:
            fillQueryPlan.setDeduplicatedDataTypes(tsDataTypes);
            fillQueryPlan.setDeduplicatedPathsAndUpdate(executePaths);
            currentDataSet = queryRouter.fill(fillQueryPlan, context);
            break;
          case QUERY:
            rawDataQueryPlan.setDeduplicatedPathsAndUpdate(executePaths);
            rawDataQueryPlan.setDeduplicatedDataTypes(tsDataTypes);
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

  /**
   * Get all measurements under given device. For a vectorMeasurementSchema, we return its
   * measurementId + all subMeasurement. e.g. schema: vector1[s1, s2], return ["vector1.s1",
   * "vector1.s2"].
   */
  protected Set<String> getMeasurementsUnderGivenDevice(PartialPath device) throws IOException {
    try {
      Set<String> res = new HashSet<>();
      // TODO: Implement this method in Cluster MManager
      List<IMeasurementSchema> measurementSchemas =
          IoTDB.metaManager.getAllMeasurementByDevicePath(device);
      for (IMeasurementSchema schema : measurementSchemas) {
        if (schema instanceof VectorMeasurementSchema) {
          for (String subMeasurement : schema.getSubMeasurementsList()) {
            res.add(schema.getMeasurementId() + TsFileConstant.PATH_SEPARATOR + subMeasurement);
          }
        } else {
          res.add(schema.getMeasurementId());
        }
      }

      return res;
    } catch (MetadataException e) {
      throw new IOException("Cannot get node from " + device, e);
    }
  }

  /**
   * Attention. For a vectorPath(root.sg.d1.vector1.s1), device is root.sg.d1, measurement is
   * "vector1.s1".
   */
  private PartialPath transformPath(PartialPath device, String measurement) throws IOException {
    try {
      PartialPath fullPath = new PartialPath(device.getFullPath(), measurement);
      IMeasurementSchema schema = IoTDB.metaManager.getSeriesSchema(fullPath);
      if (schema instanceof UnaryMeasurementSchema) {
        return fullPath;
      } else {
        String vectorPath = fullPath.getDevice();
        return new VectorPartialPath(vectorPath, fullPath.getMeasurement());
      }
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
