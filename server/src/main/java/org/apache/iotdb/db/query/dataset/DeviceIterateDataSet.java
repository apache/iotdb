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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageEngineException;
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
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * This QueryDataSet is used for GROUP_BY_DEVICE query result.
 */
public class DeviceIterateDataSet extends QueryDataSet {

  private DataSetType dataSetType;
  private IEngineQueryRouter queryRouter;
  private QueryContext context;
  private IExpression expression;

  private List<String> deduplicatedSensorColumns;
  private TreeMap<String, List<Path>> pathsGroupByDevice;
  private TreeMap<String, List<String>> aggregationsGroupByDevice;

  // group-by-time parameters
  private long unit;
  private long origin;
  private List<Pair<Long, Long>> intervals;

  // fill parameters
  private long queryTime;
  private Map<TSDataType, IFill> fillType;

  private boolean curDataSetInitialized;
  private Iterator<String> deviceIterator;
  private String currentDevice;
  private QueryDataSet currentDataSet;
  private int[] curColumnsMap;

  public DeviceIterateDataSet(QueryPlan queryPlan, QueryContext context,
      IEngineQueryRouter queryRouter) {
    this.queryRouter = queryRouter;
    this.context = context;
    this.pathsGroupByDevice = queryPlan.getPathsGroupByDevice();

    // get deduplicated sensor columns
    // Note that the deduplication strategy must be consistent with that of IoTDBQueryResultSet.
    List<String> sensorColumns = queryPlan.getSensorColumns();
    deduplicatedSensorColumns = new ArrayList<>();
    HashSet<String> tmpColumnSet = new HashSet<>();
    for (String column : sensorColumns) {
      if (!tmpColumnSet.contains(column)) {
        deduplicatedSensorColumns.add(column);
        tmpColumnSet.add(column);
      }
    }

    if (queryPlan instanceof GroupByPlan) {
      dataSetType = DataSetType.GROUPBY;
      // assign parameters
      this.expression = queryPlan.getExpression();
      this.aggregationsGroupByDevice = ((GroupByPlan) queryPlan).getAggregationsGroupByDevice();
      this.unit = ((GroupByPlan) queryPlan).getUnit();
      this.origin = ((GroupByPlan) queryPlan).getOrigin();
      this.intervals = ((GroupByPlan) queryPlan).getIntervals();

    } else if (queryPlan instanceof AggregationPlan) {
      dataSetType = DataSetType.AGGREGATE;
      // assign parameters
      this.aggregationsGroupByDevice = ((AggregationPlan) queryPlan).getAggregationsGroupByDevice();
      this.expression = queryPlan.getExpression();

    } else if (queryPlan instanceof FillQueryPlan) {
      dataSetType = DataSetType.FILL;
      // assign parameters
      this.queryTime = ((FillQueryPlan) queryPlan).getQueryTime();
      this.fillType = ((FillQueryPlan) queryPlan).getFillType();
    } else {
      dataSetType = DataSetType.QUERY;
      // assign parameters
      this.expression = queryPlan.getExpression();
    }

    this.curDataSetInitialized = false;
    this.deviceIterator = pathsGroupByDevice.keySet().iterator();
    this.curColumnsMap = new int[deduplicatedSensorColumns.size()];
  }

  public boolean hasNext() throws IOException {
    if (curDataSetInitialized && currentDataSet.hasNext()) {
      return true;
    } else {
      curDataSetInitialized = false;
    }
    for (int i = 0; i < deduplicatedSensorColumns.size(); i++) {
      curColumnsMap[i] = -1;
    }

    while (deviceIterator.hasNext()) {
      currentDevice = deviceIterator.next();
      List<Path> paths = pathsGroupByDevice.get(currentDevice);
      List<String> aggregations = null;
      if (aggregationsGroupByDevice != null) {
        aggregations = aggregationsGroupByDevice.get(currentDevice);
      }

      // get actual executed paths based on sensorColumns and get map relation
      List<String> executeColumns = new ArrayList<>();
      Set<String> columnSet = new HashSet<>();
      for (int i = 0; i < paths.size(); i++) {
        String sensor = paths.get(i).getMeasurement();
        if (aggregations != null) {
          columnSet.add(aggregations.get(i) + "(" + sensor + ")");
        } else {
          columnSet.add(sensor);
        }
      }
      int index = -1;
      for (String path : columnSet) {
        boolean isAddToExecutePaths = false;
        for (int i = 0; i < deduplicatedSensorColumns.size(); i++) {
          String column = deduplicatedSensorColumns.get(i);
          if (path.equals(column)) {
            if (!isAddToExecutePaths) {
              executeColumns.add(path);
              index++;
              isAddToExecutePaths = true;
            }
            curColumnsMap[i] = index;
          }
        }
      }
      List<Path> executePaths = new ArrayList<>();
      for (String column : executeColumns) {
        if (aggregations != null) {
          executePaths.add(new Path(currentDevice,
              column.substring(column.indexOf("(") + 1, column.indexOf(")"))));
        } else {
          executePaths.add(new Path(currentDevice, column));
        }
      }

      try {
        switch (dataSetType) {
          case GROUPBY:
            currentDataSet = queryRouter
                .groupBy(executePaths, aggregations, expression, unit, origin, intervals, context);
            break;
          case AGGREGATE:
            currentDataSet = queryRouter.aggregate(executePaths, aggregations, expression, context);
            break;
          case FILL:
            currentDataSet = queryRouter.fill(executePaths, queryTime, fillType, context);
            break;
          case QUERY:
            QueryExpression queryExpression = QueryExpression.create()
                .setSelectSeries(executePaths).setExpression(expression);
            currentDataSet = queryRouter.query(queryExpression, context);
            break;
          default:
            throw new IOException("unsupported DataSetType");
        }
      } catch (ProcessorException | QueryFilterOptimizationException | StorageEngineException |
          PathErrorException | IOException e) {
        throw new IOException(e);
      }

      if (currentDataSet.hasNext()) {
        curDataSetInitialized = true;
        return true;
      }
    }
    return false;
  }

  public RowRecord next() throws IOException {
    RowRecord rawRowRecord = currentDataSet.next();

    RowRecord rowRecord = new RowRecord(rawRowRecord.getTimestamp());

    Field deviceField = new Field(TSDataType.TEXT);
    deviceField.setBinaryV(new Binary(currentDevice));
    rowRecord.addField(deviceField);

    List<Field> sensorfields = rawRowRecord.getFields();
    for (int mapPos : curColumnsMap) {
      if (mapPos == -1) {
        rowRecord.addField(new Field(null));
      } else {
        rowRecord.addField(sensorfields.get(mapPos));
      }
    }
    return rowRecord;
  }

  private enum DataSetType {
    GROUPBY, AGGREGATE, FILL, QUERY
  }

}
