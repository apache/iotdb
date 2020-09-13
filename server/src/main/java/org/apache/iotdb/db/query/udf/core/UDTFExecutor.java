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

package org.apache.iotdb.db.query.udf.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.DataPointBatchIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.DataPointIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.DataPointSizeLimitedBatchIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.DataPointTimeWindowBatchIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordBatchIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordSizeLimitedBatchIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordTimeWindowBatchIterationStrategy;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointIterator;
import org.apache.iotdb.db.query.udf.api.iterator.Iterator;
import org.apache.iotdb.db.query.udf.datastructure.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.query.udf.datastructure.ElasticSerializableTVList;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class UDTFExecutor extends UDFExecutor {

  protected final UDTF udtf;

  protected UDTFConfigurations configurations;

  protected Map<String, ElasticSerializableRowRecordList> uniqueId2RowRecordList;

  public UDTFExecutor(UDFContext context) throws QueryProcessException {
    super(context);
    udtf = (UDTF) UDFRegistrationService.getInstance().reflect(context);
    uniqueId2RowRecordList = new HashMap<>();
  }

  @Override
  public void initializeUDF() throws QueryProcessException {
    configurations = new UDTFConfigurations(context.getPaths());
    udtf.initializeUDF(new UDFParameters(context.getPaths(), context.getAttributes()),
        configurations);
    configurations.check();
  }

  public void setupUDF(QueryContext queryContext, UDTFPlan udtfPlan,
      ElasticSerializableTVList[] rawDataColumns) throws QueryProcessException {
    try {
      udtf.setPaths(context.getPaths());
      udtf.setDataTypes(context.getDataTypes());

      setupDataPointIterators(rawDataColumns);
      setupRowRecordIterators(rawDataColumns, queryContext, udtfPlan);
      TSDataType outputDataType = configurations.getOutputDataType();
      ElasticSerializableTVList collector = new ElasticSerializableTVList(outputDataType,
          queryContext.getQueryId(), "collector",
          ElasticSerializableTVList.MEMORY_USAGE_LIMIT_FOR_SINGLE_COLUMN,
          ElasticSerializableTVList.CACHE_SIZE_FOR_SINGLE_COLUMN);
      udtf.setCollector(collector);
    } catch (MetadataException | IOException e) {
      throw new QueryProcessException(e.toString());
    }
  }

  private void setupDataPointIterators(ElasticSerializableTVList[] rawDataColumns)
      throws QueryProcessException {
    List<Iterator> dataPointIterators = new ArrayList<>();

    DataPointIterationStrategy[] strategies = configurations.getDataPointIterationStrategies();
    DataPointBatchIterationStrategy[] batchIterationStrategies = configurations
        .getDataPointBatchIterationStrategies();

    int size = strategies.length;
    for (int i = 0; i < size; ++i) {
      DataPointIterationStrategy strategy = strategies[i];
      if (strategy == null) {
        dataPointIterators.add(null);
        continue;
      }

      ElasticSerializableTVList tvList = rawDataColumns[pathIndex2DeduplicatedPathIndex.get(i)];
      switch (strategy) {
        case FETCH_BY_POINT:
          dataPointIterators.add(tvList.getDataPointIterator());
          break;
        case FETCH_BY_SIZE_LIMITED_WINDOW:
          DataPointSizeLimitedBatchIterationStrategy sizeLimitedBatchIterationStrategy
              = (DataPointSizeLimitedBatchIterationStrategy) batchIterationStrategies[i];
          int batchSize = sizeLimitedBatchIterationStrategy.getBatchSize();
          dataPointIterators.add(!sizeLimitedBatchIterationStrategy.hasDisplayWindowBegin()
              ? tvList.getSizeLimitedBatchIterator(batchSize)
              : tvList.getSizeLimitedBatchIterator(batchSize,
                  sizeLimitedBatchIterationStrategy.getDisplayWindowBegin()));
          break;
        case FETCH_BY_TIME_WINDOW:
          DataPointTimeWindowBatchIterationStrategy timeWindowBatchIterationStrategy
              = (DataPointTimeWindowBatchIterationStrategy) batchIterationStrategies[i];
          long timeInterval = timeWindowBatchIterationStrategy.getTimeInterval();
          long slidingStep = timeWindowBatchIterationStrategy.getSlidingStep();
          dataPointIterators.add(!timeWindowBatchIterationStrategy.hasDisplayWindowRange()
              ? tvList.getTimeWindowBatchIterator(timeInterval, slidingStep)
              : tvList.getTimeWindowBatchIterator(
                  timeWindowBatchIterationStrategy.getDisplayWindowBegin(),
                  timeWindowBatchIterationStrategy.getDisplayWindowEnd(),
                  timeInterval, slidingStep));
          break;
        case RANDOM_ACCESS_TO_OVERALL_DATA:
          dataPointIterators.add(tvList.asOverallDataPointIterator());
          break;
        default:
          throw new QueryProcessException("Unsupported DataPointIterationStrategy!");
      }
    }

    udtf.setDataPointIterators(dataPointIterators);
  }

  private void setupRowRecordIterators(ElasticSerializableTVList[] rawDataColumns,
      QueryContext queryContext, UDTFPlan udtfPlan) throws QueryProcessException, IOException {
    Map<String, Iterator> rowRecordIterators = new HashMap<>();

    Map<String, RowRecordIterationStrategy> strategies = configurations
        .getRowRecordIterationStrategies();
    Map<String, List<Integer>> tablets = configurations.getTablets();
    Map<String, RowRecordBatchIterationStrategy> batchIterationStrategies = configurations
        .getRowRecordBatchIterationStrategies();

    for (Entry<String, RowRecordIterationStrategy> strategy : strategies.entrySet()) {
      List<Integer> pathIndexes = tablets.get(strategy.getKey());
      ElasticSerializableRowRecordList rowRecordList = getRowRecordList(rawDataColumns,
          pathIndexes, queryContext, udtfPlan);

      switch (strategy.getValue()) {
        case FETCH_BY_ROW:
          rowRecordIterators.put(strategy.getKey(), rowRecordList.getRowRecordIterator());
          break;
        case FETCH_BY_SIZE_LIMITED_WINDOW:
          RowRecordSizeLimitedBatchIterationStrategy sizeLimitedBatchIterationStrategy =
              (RowRecordSizeLimitedBatchIterationStrategy) batchIterationStrategies
                  .get(strategy.getKey());
          int batchSize = sizeLimitedBatchIterationStrategy.getBatchSize();
          rowRecordIterators.put(strategy.getKey(),
              !sizeLimitedBatchIterationStrategy.hasDisplayWindowBegin()
                  ? rowRecordList.getSizeLimitedBatchIterator(batchSize)
                  : rowRecordList.getSizeLimitedBatchIterator(batchSize,
                      sizeLimitedBatchIterationStrategy.getDisplayWindowBegin()));
          break;
        case FETCH_BY_TIME_WINDOW:
          RowRecordTimeWindowBatchIterationStrategy timeWindowBatchIterationStrategy =
              (RowRecordTimeWindowBatchIterationStrategy) batchIterationStrategies
                  .get(strategy.getKey());
          long timeInterval = timeWindowBatchIterationStrategy.getTimeInterval();
          long slidingStep = timeWindowBatchIterationStrategy.getSlidingStep();
          rowRecordIterators.put(strategy.getKey(),
              !timeWindowBatchIterationStrategy.hasDisplayWindowRange()
                  ? rowRecordList.getTimeWindowBatchIterator(timeInterval, slidingStep)
                  : rowRecordList.getTimeWindowBatchIterator(
                      timeWindowBatchIterationStrategy.getDisplayWindowBegin(),
                      timeWindowBatchIterationStrategy.getDisplayWindowBegin(),
                      timeInterval, slidingStep));
          break;
        case RANDOM_ACCESS_TO_OVERALL_DATA:
          rowRecordIterators.put(strategy.getKey(), rowRecordList.asOverallRowRecordIterator());
          break;
        default:
          throw new QueryProcessException("Unsupported RowRecordBatchIterationStrategy!");
      }
    }

    udtf.setRowRecordIterators(rowRecordIterators);
  }

  private ElasticSerializableRowRecordList getRowRecordList(
      ElasticSerializableTVList[] rawDataColumns, List<Integer> pathIndexes,
      QueryContext queryContext, UDTFPlan udtfPlan) throws QueryProcessException, IOException {
    String uniqueId = generateUniqueId(pathIndexes);
    ElasticSerializableRowRecordList rowRecordList = uniqueId2RowRecordList.get(uniqueId);
    if (rowRecordList != null) {
      return rowRecordList;
    }

    List<TSDataType> deduplicatedDataTypes = udtfPlan.getDeduplicatedDataTypes();
    int size = pathIndexes.size();
    DataPointIterator[] rowRecordColumns = new DataPointIterator[size];
    TSDataType[] rowRecordDataTypes = new TSDataType[size];
    for (int i = 0; i < size; ++i) {
      Integer indexInDeduplicatedPath = pathIndex2DeduplicatedPathIndex.get(pathIndexes.get(i));
      rowRecordColumns[i] = rawDataColumns[indexInDeduplicatedPath].getDataPointIterator();
      rowRecordDataTypes[i] = deduplicatedDataTypes.get(indexInDeduplicatedPath);
    }

    rowRecordList = generateRowRecordList(rowRecordColumns, rowRecordDataTypes, queryContext,
        uniqueId);
    uniqueId2RowRecordList.put(uniqueId, rowRecordList);
    return rowRecordList;
  }

  private String generateUniqueId(List<Integer> pathIndexes) {
    StringBuilder builder = new StringBuilder();
    for (Integer index : pathIndexes) {
      builder.append(index).append('_');
    }
    return builder.toString();
  }

  private ElasticSerializableRowRecordList generateRowRecordList(
      DataPointIterator[] rowRecordColumns, TSDataType[] rowRecordDataTypes,
      QueryContext queryContext, String uniqueId) throws QueryProcessException, IOException {
    ElasticSerializableRowRecordList rowRecordList = new ElasticSerializableRowRecordList(
        rowRecordDataTypes, queryContext.getQueryId(), uniqueId,
        ElasticSerializableRowRecordList.MEMORY_USAGE_LIMIT_FOR_SINGLE_COLUMN,
        ElasticSerializableRowRecordList.CACHE_SIZE_FOR_SINGLE_COLUMN);
    TreeSet<Long> timeHeap = new TreeSet<>();
    for (DataPointIterator dataPointIterator : rowRecordColumns) {
      if (dataPointIterator.hasNextPoint()) {
        timeHeap.add(dataPointIterator.nextTime());
      }
    }

    int size = rowRecordColumns.length;
    while (!timeHeap.isEmpty()) {
      long minTime = timeHeap.pollFirst();
      RowRecord rowRecord = new RowRecord(minTime);

      for (int i = 0; i < size; ++i) {
        DataPointIterator dataPointIterator = rowRecordColumns[i];
        if (!dataPointIterator.hasNextPoint() || dataPointIterator.nextTime() != minTime) {
          rowRecord.addField(new Field(null));
          continue;
        }

        dataPointIterator.next();
        Object value;
        switch (rowRecordDataTypes[i]) {
          case INT32:
            value = dataPointIterator.currentInt();
            break;
          case INT64:
            value = dataPointIterator.currentLong();
            break;
          case FLOAT:
            value = dataPointIterator.currentFloat();
            break;
          case DOUBLE:
            value = dataPointIterator.currentDouble();
            break;
          case BOOLEAN:
            value = dataPointIterator.currentBoolean();
            break;
          case TEXT:
            value = dataPointIterator.currentBinary();
            break;
          default:
            throw new UnSupportedDataTypeException("Unsupported data type.");
        }
        rowRecord.addField(value, rowRecordDataTypes[i]);

        if (dataPointIterator.hasNextPoint()) {
          timeHeap.add(dataPointIterator.nextTime());
        }
      }

      rowRecordList.put(rowRecord);
    }

    return rowRecordList;
  }

  @Override
  public void executeUDF() throws QueryProcessException {
    try {
      udtf.transform();
    } catch (Exception e) {
      throw new QueryProcessException(e.toString());
    }
  }

  @Override
  public void finalizeUDF() {
    udtf.finalizeUDF();
  }

  public UDTF getUDTF() {
    return udtf;
  }

  public UDTFConfigurations getConfigurations() {
    return configurations;
  }
}
