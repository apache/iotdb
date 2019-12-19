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
package org.apache.iotdb.db.query.reader.seriesRelated;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.db.query.reader.resourceRelated.NewUnseqResourceMergeReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;

/**
 * To read series data without value filter
 *
 * "without value filter" is equivalent to "with global time filter or without any filter".
 */
public class SeriesReaderWithoutValueFilter implements IBatchReader, IPointReader {

  private IBatchReader seqResourceIterateReader;
  private IBatchReader unseqResourceMergeReader;

  // cache batch data for sequence reader
  private BatchData seqBatchData;
  // cache batch data for unsequence reader
  private BatchData unseqBatchData;

  private static final int DEFAULT_BATCH_DATA_SIZE = 10000;

  private int batchDataSize;

  /**
   * will be removed after removing IPointReader
   */
  private boolean hasCachedTimeValuePair;
  private TimeValuePair timeValuePair;
  private BatchData batchData;

  /**
   * Constructor function.
   *
   * @param seriesPath the path of the series data
   * @param timeFilter time filter condition
   * @param context query context
   * @param pushdownUnseq True to push down the filter on the unsequence TsFile resource; False not
   * to. We do not push down value filter to unsequence readers
   */
  public SeriesReaderWithoutValueFilter(Path seriesPath, TSDataType dataType, Filter timeFilter,
      QueryContext context, boolean pushdownUnseq) throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSource(seriesPath, context);
    timeFilter = queryDataSource.updateTimeFilter(timeFilter);

    // reader for sequence resources
    this.seqResourceIterateReader = new SeqResourceIterateReader(
            queryDataSource.getSeriesPath(), queryDataSource.getSeqResources(), timeFilter, context);

    // reader for unsequence resources, we only push down time filter on unseq reader
    if (pushdownUnseq) {
      this.unseqResourceMergeReader = new NewUnseqResourceMergeReader(seriesPath, dataType,
              queryDataSource.getUnseqResources(), context, timeFilter);
    } else {
      this.unseqResourceMergeReader = new NewUnseqResourceMergeReader(seriesPath, dataType,
              queryDataSource.getUnseqResources(), context, null);
    }
    this.batchDataSize = DEFAULT_BATCH_DATA_SIZE;
  }

  /**
   * for test
   */
  SeriesReaderWithoutValueFilter(IBatchReader seqResourceIterateReader,
      IBatchReader unseqResourceMergeReader) {
    this.seqResourceIterateReader = seqResourceIterateReader;
    this.unseqResourceMergeReader = unseqResourceMergeReader;
    this.batchDataSize = DEFAULT_BATCH_DATA_SIZE;
  }


  @Override
  public boolean hasNextBatch() throws IOException {
    return hasNextInSeq() || hasNextInUnSeq();
  }

  private boolean hasNextInSeq() throws IOException {
    // has next point in cached seqBatchData
    if (seqBatchData != null && seqBatchData.hasCurrent())
      return true;
    // has next batch in seq reader
    if (seqResourceIterateReader.hasNextBatch()) {
      seqBatchData = seqResourceIterateReader.nextBatch();
      return true;
    }
    return false;
  }

  private boolean hasNextInUnSeq() throws IOException {
    // has next point in cached unseqBatchData
    if (unseqBatchData != null && unseqBatchData.hasCurrent())
      return true;
    // has next batch in unseq reader
    if (unseqResourceMergeReader != null && unseqResourceMergeReader.hasNextBatch()) {
      unseqBatchData = unseqResourceMergeReader.nextBatch();
      return true;
    }
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    // has next in both seq data and unseq data
    if (hasNextInSeq() && hasNextInUnSeq()) {
      // if the count reaches batch data size
      int count = 0;
      BatchData batchData = new BatchData(seqBatchData.getDataType(), true);
      while (count < batchDataSize && hasNextInSeq() && hasNextInUnSeq()) {
        long timeInSeq = seqBatchData.currentTime();
        long timeInUnseq = unseqBatchData.currentTime();
        Object currentValue;
        long currentTime;
        if (timeInSeq < timeInUnseq) { // sequence data time is less than the unsequence data time, just use the sequence data point
          currentTime = timeInSeq;
          currentValue = seqBatchData.currentValue();
          seqBatchData.next();
        } else if (timeInSeq == timeInUnseq) { // the time equals, use the unseq data point to overwrite the seq data point
          currentTime = timeInUnseq;
          currentValue = unseqBatchData.currentValue();
          unseqBatchData.next();
          // Note that seqBatchData here still moves next even though the current data to be read is
          // overwritten by unsequence data source. Only in this way can hasNext() work correctly.
          seqBatchData.next();
        } else { // sequence data time is greater than the unsequence data time, just use the unsequence data point
          currentTime = timeInUnseq;
          currentValue = unseqBatchData.currentValue();
          unseqBatchData.next();
        }
        batchData.putTime(currentTime);
        batchData.putAnObject(currentValue);
      }
      return batchData;
    }

    // only has next in seq data
    if (hasNextInSeq())
      return seqBatchData;

    // only has next in unseq data
    if (hasNextInUnSeq())
      return unseqBatchData;

    return null;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCachedTimeValuePair) {
      return true;
    }

    if (hasNextInCurrentBatch()) {
      return true;
    }

    // has not cached timeValuePair
    if (hasNextBatch()) {
      batchData = nextBatch();
      return hasNextInCurrentBatch();
    }
    return false;
  }

  private boolean hasNextInCurrentBatch() {
    if (batchData != null && batchData.hasCurrent()) {
      timeValuePair = new TimeValuePair(batchData.currentTime(), batchData.currentTsPrimitiveType());
      hasCachedTimeValuePair = true;
      batchData.next();
      return true;
    }
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    if (hasCachedTimeValuePair || hasNext()) {
      hasCachedTimeValuePair = false;
      return timeValuePair;
    } else {
      throw new IOException("no next data");
    }
  }

  @Override
  public TimeValuePair current() {
    return timeValuePair;
  }

  @Override
  public void close() throws IOException {
    seqResourceIterateReader.close();
    unseqResourceMergeReader.close();
  }
}
