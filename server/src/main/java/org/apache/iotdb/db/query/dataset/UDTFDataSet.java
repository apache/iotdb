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

import static org.apache.iotdb.db.query.udf.datastructure.ElasticSerializableTVList.CACHE_SIZE_FOR_SINGLE_COLUMN;
import static org.apache.iotdb.db.query.udf.datastructure.ElasticSerializableTVList.MEMORY_USAGE_LIMIT_FOR_SINGLE_COLUMN;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointIterator;
import org.apache.iotdb.db.query.udf.core.UDTFExecutor;
import org.apache.iotdb.db.query.udf.datastructure.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public abstract class UDTFDataSet extends QueryDataSet {

  protected final QueryContext context;
  protected final UDTFPlan udtfPlan;

  // not null when data set executes with value filter
  protected TimeGenerator timestampGenerator;
  protected List<IReaderByTimestamp> readersByTimestamp;
  protected List<Boolean> cached;

  // not null when data set executes without value filter
  protected List<ManagedSeriesReader> managedSeriesReaders;

  // deduplicatedPaths[i] (paths[i]), deduplicatedDataTypes[i] (dataTypes[i]) -> rawDataColumns[i]
  protected ElasticSerializableTVList[] rawDataColumns;

  // a transformed data column can be either a udf query output column or a raw query output column
  protected DataPointIterator[] transformedDataColumns;
  protected TSDataType[] transformedDataColumnDataTypes;

  // execute with value filter
  public UDTFDataSet(QueryContext context, UDTFPlan udtfPlan, List<Path> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes, TimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries, List<Boolean> cached)
      throws IOException, QueryProcessException {
    super(deduplicatedPaths, deduplicatedDataTypes);
    this.context = context;
    this.udtfPlan = udtfPlan;
    this.timestampGenerator = timestampGenerator;
    this.readersByTimestamp = readersOfSelectedSeries;
    this.cached = cached;
    initRawDataColumns();
    generateRawDataWithValueFilter();
    setupAndExecuteUDFs();
    setupTransformedDataColumns();
  }

  private void initRawDataColumns() throws QueryProcessException {
    List<String> seriesReaderIdList = udtfPlan.getSeriesReaderIdList();
    int seriesNum = paths.size();
    rawDataColumns = new ElasticSerializableTVList[seriesNum];
    for (int i = 0; i < seriesNum; ++i) {
      rawDataColumns[i] = new ElasticSerializableTVList(dataTypes.get(i), context.getQueryId(),
          // series ids were generated in PhysicalGenerator
          // they make sure that each raw data column will have a unique temporary file path
          seriesReaderIdList.get(i),
          MEMORY_USAGE_LIMIT_FOR_SINGLE_COLUMN, CACHE_SIZE_FOR_SINGLE_COLUMN);
    }
  }

  private void generateRawDataWithValueFilter() throws IOException {
    int seriesNum = readersByTimestamp.size();

    while (timestampGenerator.hasNext()) {
      long timestamp = timestampGenerator.next();

      for (int i = 0; i < seriesNum; i++) {
        Object value = cached.get(i)
            ? timestampGenerator.getValue(paths.get(i), timestamp)
            : readersByTimestamp.get(i).getValueInTimestamp(timestamp);
        if (value != null) {
          rawDataColumns[i].put(timestamp, value);
        }
      }
    }
  }

  private void setupAndExecuteUDFs() throws QueryProcessException {
    for (UDTFExecutor executor : udtfPlan.getDeduplicatedExecutors()) {
      executor.setupUDF(context, udtfPlan, rawDataColumns);
    }
    for (UDTFExecutor executor : udtfPlan.getDeduplicatedExecutors()) {
      executor.executeUDF();
    }
  }

  private void setupTransformedDataColumns() {
    Map<String, Integer> pathToIndex = udtfPlan.getPathToIndex();
    transformedDataColumnDataTypes = new TSDataType[pathToIndex.size()];
    transformedDataColumns = new DataPointIterator[pathToIndex.size()];

    // UDF columns
    for (UDTFExecutor executor : udtfPlan.getDeduplicatedExecutors()) {
      int outputIndex = pathToIndex.get(executor.getContext().getColumn());
      transformedDataColumnDataTypes[outputIndex] = executor.getConfigurations()
          .getOutputDataType();
      transformedDataColumns[outputIndex] = ((ElasticSerializableTVList) executor.getUDTF()
          .getCollector()).getDataPointIterator();
    }

    // raw query columns
    List<Integer> outputIndexes = udtfPlan.getRawQueryReaderIndex2DataSetOutputColumnIndexList();
    int size = outputIndexes.size();
    for (int readerIndex = 0; readerIndex < size; ++readerIndex) {
      Integer outputIndex = outputIndexes.get(readerIndex);
      if (outputIndex == null) {
        continue;
      }
      transformedDataColumnDataTypes[outputIndex] = rawDataColumns[readerIndex].getDataType();
      transformedDataColumns[outputIndex] = rawDataColumns[readerIndex].getDataPointIterator();
    }
  }

  // execute without value filter
  public UDTFDataSet(QueryContext context, UDTFPlan udtfPlan, List<Path> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes, List<ManagedSeriesReader> readersOfSelectedSeries)
      throws QueryProcessException {
    super(deduplicatedPaths, deduplicatedDataTypes);
    this.context = context;
    this.udtfPlan = udtfPlan;
    this.managedSeriesReaders = readersOfSelectedSeries;
    initRawDataColumns();
    generateRawDataWithoutValueFilter();
    setupAndExecuteUDFs();
    setupTransformedDataColumns();
  }

  private void generateRawDataWithoutValueFilter() throws QueryProcessException {
    try {
      (new RawQueryDataSetWithoutValueFilter(paths, dataTypes, managedSeriesReaders) {

        public void fillUDFRawDataColumns() throws IOException, InterruptedException {
          int seriesNum = seriesReaderList.size();

          while (!timeHeap.isEmpty()) {
            long timestamp = timeHeap.pollFirst();

            for (int i = 0; i < seriesNum; ++i) {
              if (cachedBatchDataArray[i] != null && cachedBatchDataArray[i].hasCurrent()
                  && cachedBatchDataArray[i].currentTime() == timestamp) {
                TSDataType type = cachedBatchDataArray[i].getDataType();
                switch (type) {
                  case INT32:
                    rawDataColumns[i].putInt(timestamp, cachedBatchDataArray[i].getInt());
                    break;
                  case INT64:
                    rawDataColumns[i].putLong(timestamp, cachedBatchDataArray[i].getLong());
                    break;
                  case FLOAT:
                    rawDataColumns[i].putFloat(timestamp, cachedBatchDataArray[i].getFloat());
                    break;
                  case DOUBLE:
                    rawDataColumns[i].putDouble(timestamp, cachedBatchDataArray[i].getDouble());
                    break;
                  case BOOLEAN:
                    rawDataColumns[i].putBoolean(timestamp, cachedBatchDataArray[i].getBoolean());
                    break;
                  case TEXT:
                    rawDataColumns[i].putBinary(timestamp, cachedBatchDataArray[i].getBinary());
                    break;
                  default:
                    throw new UnSupportedDataTypeException(
                        String.format("Data type %s is not supported.", type));
                }

                // move next
                cachedBatchDataArray[i].next();

                // get next batch if current batch is empty and still have remaining batch data in queue
                if (!cachedBatchDataArray[i].hasCurrent() && !noMoreDataInQueueArray[i]) {
                  fillCache(i);
                }

                // try to put the next timestamp into the heap
                if (cachedBatchDataArray[i].hasCurrent()) {
                  timeHeap.add(cachedBatchDataArray[i].currentTime());
                }
              }
            }
          }
        }
      }).fillUDFRawDataColumns();
    } catch (IOException | InterruptedException e) {
      throw new QueryProcessException(e.toString());
    }
  }

  public void finalizeUDFs() {
    udtfPlan.finalizeUDFExecutors();
  }
}
