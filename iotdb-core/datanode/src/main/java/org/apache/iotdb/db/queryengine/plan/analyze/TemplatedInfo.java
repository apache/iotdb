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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand.TIMESTAMP_EXPRESSION_STRING;

/**
 * If in align by device query ,all queried devices are set in one template, we can store the common
 * variables in TemplatedInfo to avoid repeated creation.
 */
public class TemplatedInfo {

  // measurement info
  private final List<String> measurementList;
  private final List<IMeasurementSchema> schemaList;
  private final List<TSDataType> dataTypes;

  // query info
  private final Ordering scanOrder;
  private final boolean queryAllSensors;

  // variables used in DeviceViewOperator
  private final List<String> deviceViewOutputNames;
  private List<Integer> deviceToMeasurementIndexes;

  // variables related to LIMIT/OFFSET push down
  private final long offsetValue;
  private final long limitValue;

  // variables used in FilterOperator
  private final Expression predicate;
  private boolean keepNull;

  // utils variables, not serialize
  private Map<String, IMeasurementSchema> schemaMap;
  private Map<String, List<InputLocation>> filterLayoutMap;
  private int maxTsBlockLineNum = -1;

  // variables related to predicate push down
  private Expression pushDownPredicate;

  // variables related to aggregation
  private final boolean aggregationQuery;
  private final GroupByTimeParameter groupByTimeParameter;
  private final boolean outputEndTime;
  private List<AggregationDescriptor> ascendingDescriptorList;
  private List<AggregationDescriptor> descendingDescriptorList;

  public TemplatedInfo(
      List<String> measurementList,
      List<IMeasurementSchema> schemaList,
      List<TSDataType> dataTypes,
      Ordering scanOrder,
      boolean queryAllSensors,
      List<String> deviceViewOutputNames,
      List<Integer> deviceToMeasurementIndexes,
      long offsetValue,
      long limitValue,
      Expression predicate,
      boolean keepNull,
      Map<String, IMeasurementSchema> schemaMap,
      Map<String, List<InputLocation>> filterLayoutMap,
      Expression pushDownPredicate,
      boolean aggregationQuery,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime) {
    this.measurementList = measurementList;
    this.schemaList = schemaList;
    this.dataTypes = dataTypes;
    this.scanOrder = scanOrder;
    this.queryAllSensors = queryAllSensors;
    this.deviceViewOutputNames = deviceViewOutputNames;
    this.deviceToMeasurementIndexes = deviceToMeasurementIndexes;
    this.offsetValue = offsetValue;
    this.limitValue = limitValue;
    this.predicate = predicate;
    if (predicate != null) {
      this.keepNull = keepNull;
      this.filterLayoutMap = filterLayoutMap;
    }
    this.pushDownPredicate = pushDownPredicate;
    this.schemaMap = schemaMap;

    this.aggregationQuery = aggregationQuery;
    this.groupByTimeParameter = groupByTimeParameter;
    this.outputEndTime = outputEndTime;
  }

  public List<String> getMeasurementList() {
    return this.measurementList;
  }

  public List<IMeasurementSchema> getSchemaList() {
    return this.schemaList;
  }

  public List<TSDataType> getDataTypes() {
    return this.dataTypes;
  }

  public Ordering getScanOrder() {
    return this.scanOrder;
  }

  public boolean isQueryAllSensors() {
    return this.queryAllSensors;
  }

  public List<String> getDeviceViewOutputNames() {
    return this.deviceViewOutputNames;
  }

  public long getOffsetValue() {
    return this.offsetValue;
  }

  public long getLimitValue() {
    return this.limitValue;
  }

  public List<Integer> getDeviceToMeasurementIndexes() {
    return this.deviceToMeasurementIndexes;
  }

  public void setDeviceToMeasurementIndexes(List<Integer> deviceToMeasurementIndexes) {
    this.deviceToMeasurementIndexes = deviceToMeasurementIndexes;
  }

  public Expression getPredicate() {
    return this.predicate;
  }

  public boolean isKeepNull() {
    return this.keepNull;
  }

  public Map<String, IMeasurementSchema> getSchemaMap() {
    return this.schemaMap;
  }

  public Map<String, List<InputLocation>> getFilterLayoutMap() {
    return this.filterLayoutMap;
  }

  public Expression getPushDownPredicate() {
    return this.pushDownPredicate;
  }

  public void setPushDownPredicate(Expression pushDownPredicate) {
    this.pushDownPredicate = pushDownPredicate;
  }

  public boolean hasPushDownPredicate() {
    return this.pushDownPredicate != null;
  }

  public Expression[] getProjectExpressions() {
    Expression[] projectExpressions = new Expression[measurementList.size()];
    for (int i = 0; i < measurementList.size(); i++) {
      projectExpressions[i] =
          new TimeSeriesOperand(
              new PartialPath(new String[] {measurementList.get(i)}), schemaList.get(i).getType());
    }
    return projectExpressions;
  }

  public boolean isAggregationQuery() {
    return aggregationQuery;
  }

  public GroupByTimeParameter getGroupByTimeParameter() {
    return this.groupByTimeParameter;
  }

  public boolean isOutputEndTime() {
    return outputEndTime;
  }

  public List<AggregationDescriptor> getAscendingDescriptorList() {
    return this.ascendingDescriptorList;
  }

  public void setAscendingDescriptorList(List<AggregationDescriptor> ascendingDescriptorList) {
    this.ascendingDescriptorList = ascendingDescriptorList;
  }

  public List<AggregationDescriptor> getDescendingDescriptorList() {
    return this.descendingDescriptorList;
  }

  public void setDescendingDescriptorList(List<AggregationDescriptor> descendingDescriptorList) {
    this.descendingDescriptorList = descendingDescriptorList;
  }

  public static Map<String, List<InputLocation>> makeLayout(List<String> measurementList) {
    Map<String, List<InputLocation>> outputMappings = new LinkedHashMap<>();
    int tsBlockIndex = 0;
    outputMappings
        .computeIfAbsent(TIMESTAMP_EXPRESSION_STRING, key -> new ArrayList<>())
        .add(new InputLocation(tsBlockIndex, -1));
    int valueColumnIndex = 0;
    for (String columnName : measurementList) {
      outputMappings
          .computeIfAbsent(columnName, key -> new ArrayList<>())
          .add(new InputLocation(tsBlockIndex, valueColumnIndex));
      valueColumnIndex++;
    }
    return outputMappings;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(measurementList.size(), byteBuffer);
    for (String measurement : measurementList) {
      ReadWriteIOUtils.write(measurement, byteBuffer);
    }
    for (IMeasurementSchema schema : schemaList) {
      schema.serializeTo(byteBuffer);
    }
    for (TSDataType dataType : dataTypes) {
      ReadWriteIOUtils.write(dataType, byteBuffer);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(queryAllSensors, byteBuffer);

    ReadWriteIOUtils.write(deviceViewOutputNames.size(), byteBuffer);
    for (String outputName : deviceViewOutputNames) {
      ReadWriteIOUtils.write(outputName, byteBuffer);
    }

    ReadWriteIOUtils.write(deviceToMeasurementIndexes.size(), byteBuffer);
    for (int index : deviceToMeasurementIndexes) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }

    ReadWriteIOUtils.write(offsetValue, byteBuffer);
    ReadWriteIOUtils.write(limitValue, byteBuffer);

    if (predicate != null) {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      Expression.serialize(predicate, byteBuffer);
      ReadWriteIOUtils.write(keepNull, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }

    if (pushDownPredicate != null) {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      Expression.serialize(pushDownPredicate, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }

    if (aggregationQuery) {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      if (groupByTimeParameter != null) {
        ReadWriteIOUtils.write((byte) 1, byteBuffer);
        groupByTimeParameter.serialize(byteBuffer);
      } else {
        ReadWriteIOUtils.write((byte) 0, byteBuffer);
      }

      ReadWriteIOUtils.write(outputEndTime, byteBuffer);

      ReadWriteIOUtils.write(ascendingDescriptorList.size(), byteBuffer);
      for (AggregationDescriptor descriptor : ascendingDescriptorList) {
        descriptor.serialize(byteBuffer);
      }

      ReadWriteIOUtils.write(descendingDescriptorList.size(), byteBuffer);
      for (AggregationDescriptor descriptor : descendingDescriptorList) {
        descriptor.serialize(byteBuffer);
      }
    } else {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(measurementList.size(), stream);
    for (String measurement : measurementList) {
      ReadWriteIOUtils.write(measurement, stream);
    }
    for (IMeasurementSchema schema : schemaList) {
      schema.serializeTo(stream);
    }
    for (TSDataType dataType : dataTypes) {
      ReadWriteIOUtils.write(dataType, stream);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
    ReadWriteIOUtils.write(queryAllSensors, stream);

    ReadWriteIOUtils.write(deviceViewOutputNames.size(), stream);
    for (String outputName : deviceViewOutputNames) {
      ReadWriteIOUtils.write(outputName, stream);
    }

    ReadWriteIOUtils.write(deviceToMeasurementIndexes.size(), stream);
    for (int index : deviceToMeasurementIndexes) {
      ReadWriteIOUtils.write(index, stream);
    }

    ReadWriteIOUtils.write(offsetValue, stream);
    ReadWriteIOUtils.write(limitValue, stream);

    if (predicate != null) {
      ReadWriteIOUtils.write((byte) 1, stream);
      Expression.serialize(predicate, stream);
      ReadWriteIOUtils.write(keepNull, stream);
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }

    if (pushDownPredicate != null) {
      ReadWriteIOUtils.write((byte) 1, stream);
      Expression.serialize(pushDownPredicate, stream);
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }

    if (aggregationQuery) {
      ReadWriteIOUtils.write((byte) 1, stream);
      if (groupByTimeParameter != null) {
        ReadWriteIOUtils.write((byte) 1, stream);
        groupByTimeParameter.serialize(stream);
      } else {
        ReadWriteIOUtils.write((byte) 0, stream);
      }

      ReadWriteIOUtils.write(outputEndTime, stream);

      ReadWriteIOUtils.write(ascendingDescriptorList.size(), stream);
      for (AggregationDescriptor descriptor : ascendingDescriptorList) {
        descriptor.serialize(stream);
      }

      ReadWriteIOUtils.write(descendingDescriptorList.size(), stream);
      for (AggregationDescriptor descriptor : descendingDescriptorList) {
        descriptor.serialize(stream);
      }
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }
  }

  public static TemplatedInfo deserialize(ByteBuffer byteBuffer) {

    int measurementSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> measurementList = new ArrayList<>();
    int cnt = measurementSize;
    while (cnt-- > 0) {
      measurementList.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    cnt = measurementSize;
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    while (cnt-- > 0) {
      // MeasurementSchema is ok?
      measurementSchemaList.add(MeasurementSchema.deserializeFrom(byteBuffer));
    }

    cnt = measurementSize;
    List<TSDataType> dataTypeList = new ArrayList<>();
    while (cnt-- > 0) {
      dataTypeList.add(ReadWriteIOUtils.readDataType(byteBuffer));
    }

    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];

    boolean queryAllSensors = ReadWriteIOUtils.readBool(byteBuffer);

    int listSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> selectMeasurements = new ArrayList<>(listSize);
    while (listSize-- > 0) {
      selectMeasurements.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    listSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Integer> deviceToMeasurementIndexes = new ArrayList<>(listSize);
    while (listSize-- > 0) {
      deviceToMeasurementIndexes.add(ReadWriteIOUtils.readInt(byteBuffer));
    }

    long offsetValue = ReadWriteIOUtils.readLong(byteBuffer);

    long limitValue = ReadWriteIOUtils.readLong(byteBuffer);

    Expression predicate = null;
    boolean keepNull = false;
    Map<String, List<InputLocation>> filterLayoutMap = null;
    byte hasFilter = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasFilter == 1) {
      predicate = Expression.deserialize(byteBuffer);
      keepNull = ReadWriteIOUtils.readBool(byteBuffer);
      filterLayoutMap = makeLayout(measurementList);
    }

    Map<String, IMeasurementSchema> measurementSchemaMap =
        new HashMap<>(measurementSchemaList.size());
    measurementSchemaList.forEach(
        measurementSchema ->
            measurementSchemaMap.put(measurementSchema.getMeasurementId(), measurementSchema));

    Expression pushDownPredicate = null;
    byte hasPushDownFilter = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasPushDownFilter == 1) {
      pushDownPredicate = Expression.deserialize(byteBuffer);
    }

    boolean aggregationQuery = ReadWriteIOUtils.readBool(byteBuffer);
    GroupByTimeParameter groupByTimeParameter = null;
    boolean outputEndTime = false;
    List<AggregationDescriptor> ascendingDescriptorList = null;
    List<AggregationDescriptor> descendingDescriptorList = null;

    if (aggregationQuery) {
      byte hasGroupByTime = ReadWriteIOUtils.readByte(byteBuffer);
      if (hasGroupByTime == 1) {
        groupByTimeParameter = GroupByTimeParameter.deserialize(byteBuffer);
      }

      outputEndTime = ReadWriteIOUtils.readBool(byteBuffer);

      int size = ReadWriteIOUtils.readInt(byteBuffer);
      ascendingDescriptorList = new ArrayList<>(size);
      while (size-- > 0) {
        ascendingDescriptorList.add(AggregationDescriptor.deserialize(byteBuffer));
      }

      size = ReadWriteIOUtils.readInt(byteBuffer);
      descendingDescriptorList = new ArrayList<>(size);
      while (size-- > 0) {
        descendingDescriptorList.add(AggregationDescriptor.deserialize(byteBuffer));
      }
    }

    TemplatedInfo templatedInfo =
        new TemplatedInfo(
            measurementList,
            measurementSchemaList,
            dataTypeList,
            scanOrder,
            queryAllSensors,
            selectMeasurements,
            deviceToMeasurementIndexes,
            offsetValue,
            limitValue,
            predicate,
            keepNull,
            measurementSchemaMap,
            filterLayoutMap,
            pushDownPredicate,
            aggregationQuery,
            groupByTimeParameter,
            outputEndTime);
    templatedInfo.setAscendingDescriptorList(ascendingDescriptorList);
    templatedInfo.setDescendingDescriptorList(descendingDescriptorList);
    return templatedInfo;
  }
}
