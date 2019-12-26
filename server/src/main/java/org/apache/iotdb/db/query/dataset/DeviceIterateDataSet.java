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
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.IEngineQueryRouter;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;


/**
 * This QueryDataSet is used for GROUP_BY_DEVICE query result.
 */
public class DeviceIterateDataSet extends QueryDataSet {

  private DataSetType dataSetType;
  private IEngineQueryRouter queryRouter;
  private QueryContext context;
  private IExpression expression;

  private List<String> deduplicatedMeasurementColumns;
  private Map<String, Set<String>> measurementColumnsGroupByDevice;

  // group-by-time parameters
  private long unit;
  private long slidingStep;
  private long startTime;
  private long endTime;

  // fill parameters
  private long queryTime;
  private Map<TSDataType, IFill> fillType;

  private boolean curDataSetInitialized;
  private Iterator<String> deviceIterator;
  private String currentDevice;
  private QueryDataSet currentDataSet;
  private int[] currentColumnMapRelation;

  public DeviceIterateDataSet(QueryPlan queryPlan, QueryContext context,
      IEngineQueryRouter queryRouter) {
    super(null, queryPlan.getDataTypes());

    // get deduplicated measurement columns (already deduplicated in TSServiceImpl.executeDataQuery)
    this.deduplicatedMeasurementColumns = queryPlan.getMeasurementColumnList();

    this.queryRouter = queryRouter;
    this.context = context;
    this.measurementColumnsGroupByDevice = queryPlan.getMeasurementColumnsGroupByDevice();

    if (queryPlan instanceof GroupByPlan) {
      this.dataSetType = DataSetType.GROUPBY;
      // assign parameters
      this.expression = queryPlan.getExpression();
      this.unit = ((GroupByPlan) queryPlan).getUnit();
      this.slidingStep = ((GroupByPlan) queryPlan).getSlidingStep();
      this.startTime = ((GroupByPlan) queryPlan).getStartTime();
      this.endTime = ((GroupByPlan) queryPlan).getEndTime();

    } else if (queryPlan instanceof AggregationPlan) {
      this.dataSetType = DataSetType.AGGREGATE;
      // assign parameters
      this.expression = queryPlan.getExpression();

    } else if (queryPlan instanceof FillQueryPlan) {
      this.dataSetType = DataSetType.FILL;
      // assign parameters
      this.queryTime = ((FillQueryPlan) queryPlan).getQueryTime();
      this.fillType = ((FillQueryPlan) queryPlan).getFillType();
    } else {
      this.dataSetType = DataSetType.QUERY;
      // assign parameters
      this.expression = queryPlan.getExpression();
    }

    this.curDataSetInitialized = false;
    this.deviceIterator = measurementColumnsGroupByDevice.keySet().iterator();
    this.currentColumnMapRelation = new int[deduplicatedMeasurementColumns.size()];
  }

  protected boolean hasNextWithoutConstraint() throws IOException {
    if (curDataSetInitialized && currentDataSet.hasNext()) {
      return true;
    } else {
      curDataSetInitialized = false;
    }
    for (int i = 0; i < deduplicatedMeasurementColumns.size(); i++) {
      currentColumnMapRelation[i] = -1;
    }

    while (deviceIterator.hasNext()) {
      currentDevice = deviceIterator.next();
      Set<String> measurementColumnsOfGivenDevice = measurementColumnsGroupByDevice
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
      List<String> executeAggregations = new ArrayList<>();
      for (String column : executeColumns) {
        if (dataSetType == DataSetType.GROUPBY || dataSetType == DataSetType.AGGREGATE) {
          executePaths.add(new Path(currentDevice,
              column.substring(column.indexOf("(") + 1, column.indexOf(")"))));
          executeAggregations.add(column.substring(0, column.indexOf("(")));
        } else {
          executePaths.add(new Path(currentDevice, column));
        }
      }

      try {
        switch (dataSetType) {
          case GROUPBY:
            currentDataSet = queryRouter
                .groupBy(executePaths, dataTypes, executeAggregations, expression, unit,
                    slidingStep,
                    startTime, endTime, context);
            break;
          case AGGREGATE:
            currentDataSet = queryRouter
                .aggregate(executePaths, dataTypes, executeAggregations, expression, context);
            break;
          case FILL:
            currentDataSet = queryRouter
                .fill(executePaths, dataTypes, queryTime, fillType, context);
            break;
          case QUERY:
            currentDataSet = queryRouter.query(executePaths, dataTypes, expression, context);
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
        rowRecord.addField(new Field(null));
      } else {
        rowRecord.addField(measurementFields.get(mapPos));
      }
    }
    return rowRecord;
  }

  private enum DataSetType {
    GROUPBY, AGGREGATE, FILL, QUERY
  }

}
