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

package org.apache.tsfile.read.reader.block;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.query.executor.task.DeviceQueryTask;
import org.apache.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.tsfile.read.reader.series.FileSeriesReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

public class SingleDeviceTsBlockReader implements TsBlockReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleDeviceTsBlockReader.class);
  private final DeviceQueryTask task;
  private final ExpressionTree measurementExpression;
  private final int blockSize;

  private final TsBlock currentBlock;
  private boolean lastBlockReturned = true;
  private final Map<String, MeasurementColumnContext> measureColumnContextMap;
  private final Map<String, IdColumnContext> idColumnContextMap;

  private long nextTime;

  public SingleDeviceTsBlockReader(
      DeviceQueryTask task,
      IMetadataQuerier metadataQuerier,
      IChunkLoader chunkLoader,
      int blockSize,
      ExpressionTree timeExpression,
      ExpressionTree measurementFilter)
      throws IOException {
    this.task = task;
    this.blockSize = blockSize;
    this.measurementExpression = measurementFilter;

    this.currentBlock =
        TsBlock.buildTsBlock(task.getColumnNames(), task.getTableSchema(), blockSize);
    this.measureColumnContextMap = new HashMap<>();
    this.idColumnContextMap = new HashMap<>();

    final List<List<IChunkMetadata>> chunkMetadataLists =
        metadataQuerier.getChunkMetadataLists(
            task.getDeviceID(),
            task.getColumnMapping().getMeasurementColumns(),
            task.getIndexRoot());

    Filter timeFilter = timeExpression == null ? null : timeExpression.toFilter();
    for (List<IChunkMetadata> chunkMetadataList : chunkMetadataLists) {
      constructColumnContext(chunkMetadataList, chunkLoader, timeFilter);
    }

    for (String idColumn : task.getColumnMapping().getIdColumns()) {
      final List<Integer> columnPosInResult = task.getColumnMapping().getColumnPos(idColumn);
      // the first segment in DeviceId is the table name
      final int columnPosInId = task.getTableSchema().findIdColumnOrder(idColumn) + 1;
      idColumnContextMap.put(idColumn, new IdColumnContext(columnPosInResult, columnPosInId));
    }
  }

  private void constructColumnContext(
      List<IChunkMetadata> chunkMetadataList, IChunkLoader chunkLoader, Filter timeFilter)
      throws IOException {
    if (chunkMetadataList.isEmpty()) {
      return;
    }
    final IChunkMetadata chunkMetadata = chunkMetadataList.get(0);
    AbstractFileSeriesReader seriesReader =
        new FileSeriesReader(chunkLoader, chunkMetadataList, timeFilter);
    if (seriesReader.hasNextBatch()) {
      if (chunkMetadata instanceof AlignedChunkMetadata) {
        final List<String> currentChunkMeasurementNames =
            seriesReader.getCurrentChunkMeasurementNames();
        List<List<Integer>> posInResult = new ArrayList<>();
        for (String currentChunkMeasurementName : currentChunkMeasurementNames) {
          posInResult.add(task.getColumnMapping().getColumnPos(currentChunkMeasurementName));
        }
        measureColumnContextMap.put(
            "",
            new VectorMeasurementColumnContext(
                posInResult, seriesReader.nextBatch(), seriesReader));
      } else {
        final String measurementUid = chunkMetadata.getMeasurementUid();
        measureColumnContextMap.put(
            measurementUid,
            new SingleMeasurementColumnContext(
                measurementUid,
                task.getColumnMapping().getColumnPos(measurementUid),
                seriesReader.nextBatch(),
                seriesReader));
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (!lastBlockReturned) {
      return true;
    }

    if (measureColumnContextMap.isEmpty()) {
      return false;
    }

    currentBlock.reset();
    nextTime = Long.MAX_VALUE;
    List<MeasurementColumnContext> minTimeColumns = new ArrayList<>();

    while (currentBlock.getPositionCount() < blockSize) {
      // find the minimum time among the batches and the associated columns
      for (Entry<String, MeasurementColumnContext> entry : measureColumnContextMap.entrySet()) {
        final BatchData batchData = entry.getValue().currentBatch;
        final long currentTime = batchData.currentTime();
        if (nextTime > currentTime) {
          nextTime = currentTime;
          minTimeColumns.clear();
          minTimeColumns.add(entry.getValue());
        } else if (nextTime == currentTime) {
          minTimeColumns.add(entry.getValue());
        }
      }

      try {
        fillMeasurements(minTimeColumns);
        nextTime = Long.MAX_VALUE;
      } catch (IOException e) {
        LOGGER.error("Cannot fill measurements", e);
        return false;
      }

      // all columns have exhausted
      if (measureColumnContextMap.isEmpty()) {
        break;
      }
    }

    if (currentBlock.getPositionCount() > 0) {
      fillIds();
      currentBlock.fillTrailingNulls();
      lastBlockReturned = false;
      return true;
    }

    return false;
  }

  private void fillIds() {
    for (Entry<String, IdColumnContext> entry : idColumnContextMap.entrySet()) {
      final IdColumnContext idColumnContext = entry.getValue();
      for (Integer pos : idColumnContext.posInResult) {
        final Column column = currentBlock.getColumn(pos);
        fillIdColumn(
            column,
            task.getDeviceID().segment(idColumnContext.posInDeviceId),
            0,
            currentBlock.getPositionCount());
      }
    }
  }

  private void fillMeasurements(List<MeasurementColumnContext> minTimeColumns) throws IOException {
    if (measurementExpression == null || measurementExpression.satisfy(this)) {
      // use the time to fill the block
      final int positionCount = currentBlock.getPositionCount();
      currentBlock.getTimeColumn().getLongs()[positionCount] = nextTime;
      // project the value columns to the result
      for (final MeasurementColumnContext columnContext : minTimeColumns) {
        columnContext.fillInto(currentBlock, positionCount);
        advanceColumn(columnContext.currentBatch, columnContext);
      }
      currentBlock.setPositionCount(positionCount + 1);
    } else {
      for (final MeasurementColumnContext columnContext : minTimeColumns) {
        final BatchData batchData = columnContext.currentBatch;
        advanceColumn(batchData, columnContext);
      }
    }
  }

  private void advanceColumn(BatchData batchData, MeasurementColumnContext columnContext)
      throws IOException {
    batchData.next();
    if (!batchData.hasCurrent()) {
      // get next batch of the column
      if (columnContext.seriesReader.hasNextBatch()) {
        columnContext.currentBatch = columnContext.seriesReader.nextBatch();
      } else {
        // no more data in this column
        columnContext.removeFrom(measureColumnContextMap);
      }
    }
  }

  private void fillIdColumn(Column column, Object val, int startPos, int endPos) {
    switch (column.getDataType()) {
      case TEXT:
        if (val instanceof String) {
          val = new Binary(((String) val), StandardCharsets.UTF_8);
        }
        Arrays.fill(column.getBinaries(), startPos, endPos, val);
        break;
      case BOOLEAN:
        Arrays.fill(column.getBooleans(), startPos, endPos, ((boolean) val));
        break;
      case INT32:
        Arrays.fill(column.getInts(), startPos, endPos, ((int) val));
        break;
      case INT64:
        Arrays.fill(column.getLongs(), startPos, endPos, ((long) val));
        break;
      case FLOAT:
        Arrays.fill(column.getFloats(), startPos, endPos, ((float) val));
        break;
      case DOUBLE:
        Arrays.fill(column.getDoubles(), startPos, endPos, ((double) val));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + column.getDataType());
    }
    column.setPositionCount(endPos);
  }

  private static void fillSingleMeasurementColumn(Column column, BatchData batchData, int pos) {
    switch (batchData.getDataType()) {
      case BOOLEAN:
        column.getBooleans()[pos] = batchData.getBoolean();
        break;
      case DOUBLE:
        column.getDoubles()[pos] = batchData.getDouble();
        break;
      case FLOAT:
        column.getFloats()[pos] = batchData.getFloat();
        break;
      case INT32:
        column.getInts()[pos] = batchData.getInt();
        break;
      case TEXT:
        column.getBinaries()[pos] = batchData.getBinary();
        break;
      case INT64:
        column.getLongs()[pos] = batchData.getLong();
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + batchData.getDataType());
    }
    column.setPositionCount(pos + 1);
  }

  @Override
  public TsBlock next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    lastBlockReturned = true;
    return currentBlock;
  }

  @Override
  public void close() throws Exception {
    // nothing to be done
  }

  public abstract static class MeasurementColumnContext {

    protected BatchData currentBatch;
    protected final AbstractFileSeriesReader seriesReader;

    protected MeasurementColumnContext(
        AbstractFileSeriesReader seriesReader, BatchData currentBatch) {
      this.seriesReader = seriesReader;
      this.currentBatch = currentBatch;
    }

    abstract void removeFrom(Map<String, MeasurementColumnContext> columnContextMap);

    abstract void fillInto(TsBlock block, int position);
  }

  // gather necessary fields in this class to avoid redundant map access
  public static class SingleMeasurementColumnContext extends MeasurementColumnContext {

    private final String columnName;
    private final List<Integer> posInResult;

    public SingleMeasurementColumnContext(
        String columnName,
        List<Integer> posInResult,
        BatchData currentBatch,
        AbstractFileSeriesReader seriesReader) {
      super(seriesReader, currentBatch);
      this.columnName = columnName;
      this.posInResult = posInResult;
    }

    @Override
    void removeFrom(Map<String, MeasurementColumnContext> columnContextMap) {
      columnContextMap.remove(columnName);
    }

    @Override
    void fillInto(TsBlock block, int position) {
      for (Integer pos : posInResult) {
        final Column column = block.getColumn(pos);
        fillSingleMeasurementColumn(column, currentBatch, position);
      }
    }
  }

  public static class VectorMeasurementColumnContext extends MeasurementColumnContext {

    private final List<List<Integer>> posInResult;

    public VectorMeasurementColumnContext(
        List<List<Integer>> posInResult,
        BatchData currentBatch,
        AbstractFileSeriesReader seriesReader) {
      super(seriesReader, currentBatch);
      this.posInResult = posInResult;
    }

    @Override
    void removeFrom(Map<String, MeasurementColumnContext> columnContextMap) {
      columnContextMap.remove("");
    }

    @Override
    void fillInto(TsBlock block, int blockRowNum) {
      final TsPrimitiveType[] vector = currentBatch.getVector();
      for (int i = 0; i < vector.length; i++) {
        final TsPrimitiveType value = vector[i];
        final List<Integer> columnPositions = posInResult.get(i);
        for (Integer pos : columnPositions) {
          switch (value.getDataType()) {
            case TEXT:
              block.getColumn(pos).getBinaries()[blockRowNum] = value.getBinary();
              break;
            case INT32:
              block.getColumn(pos).getInts()[blockRowNum] = value.getInt();
              break;
            case INT64:
              block.getColumn(pos).getLongs()[blockRowNum] = value.getLong();
              break;
            case BOOLEAN:
              block.getColumn(pos).getBooleans()[blockRowNum] = value.getBoolean();
              break;
            case FLOAT:
              block.getColumn(pos).getFloats()[blockRowNum] = value.getFloat();
              break;
            case DOUBLE:
              block.getColumn(pos).getDoubles()[blockRowNum] = value.getDouble();
              break;
            default:
              throw new IllegalArgumentException("Unsupported data type: " + value.getDataType());
          }
          block.getColumn(pos).setPositionCount(blockRowNum + 1);
        }
      }
    }
  }

  public static class IdColumnContext {

    private final List<Integer> posInResult;
    private final int posInDeviceId;

    public IdColumnContext(List<Integer> posInResult, int posInDeviceId) {
      this.posInResult = posInResult;
      this.posInDeviceId = posInDeviceId;
    }
  }
}
