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

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceMergeReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * To read series data without value filter, this class implements {@link IPointReader} for the
 * data.
 * <p>
 * Note that filters include value filter and time filter. "without value filter" is equivalent to
 * "with global time filter or simply without any filter".
 */


/**
 * merge seqResourceIterateReader and unseqResourceMergeReader
 *
 * return batch data
 */
public class SeriesReaderWithoutValueFilter {

  private IBatchReader seqResourceIterateReader;
  private IBatchReader unseqResourceMergeReader;

  private boolean hasCachedBatchData;
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
  public SeriesReaderWithoutValueFilter(Path seriesPath, Filter timeFilter, QueryContext context,
      boolean pushdownUnseq) throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context);
    timeFilter = queryDataSource.updateTimeFilter(timeFilter);

    // reader for sequence resources
    this.seqResourceIterateReader = new SeqResourceIterateReader(
        queryDataSource.getSeriesPath(), queryDataSource.getSeqResources(), timeFilter, context);

    // reader for unsequence resources, we only push down time filter on unseq reader
    if (pushdownUnseq) {
      this.unseqResourceMergeReader = new UnseqResourceMergeReader(seriesPath,
          queryDataSource.getUnseqResources(), context, timeFilter);
    } else {
      this.unseqResourceMergeReader = new UnseqResourceMergeReader(seriesPath,
          queryDataSource.getUnseqResources(), context, null);
    }

    this.hasCachedBatchData = false;
  }

  /**
   * for test
   */
  SeriesReaderWithoutValueFilter(IBatchReader seqResourceIterateReader,
      IBatchReader unseqResourceMergeReader) {
    this.seqResourceIterateReader = seqResourceIterateReader;
    this.unseqResourceMergeReader = unseqResourceMergeReader;
    this.hasCachedBatchData = false;
  }


  /**
   * methods in IPointReader
   */
//  @Override
//  public boolean hasNext() throws IOException {
//    if (hasNextInBatchDataOrBatchReader()) {
//      return true;
//    }
//    return unseqResourceMergeReader != null && unseqResourceMergeReader.hasNext();
//  }
//
//  @Override
//  public TimeValuePair next() throws IOException {
//    boolean hasNextBatch = hasNextInBatchDataOrBatchReader();
//    boolean hasNextPoint = unseqResourceMergeReader != null && unseqResourceMergeReader.hasNext();
//
//    // has next in both batch reader and point reader
//    if (hasNextBatch && hasNextPoint) {
//      long timeInPointReader = unseqResourceMergeReader.current().getTimestamp();
//      long timeInBatchData = batchData.currentTime();
//      if (timeInPointReader > timeInBatchData) {
//        TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
//        batchData.next();
//        return timeValuePair;
//      } else if (timeInPointReader == timeInBatchData) {
//        // Note that batchData here still moves next even though the current data to be read is
//        // overwritten by unsequence data source. Only in this way can hasNext() work correctly.
//        batchData.next();
//        return unseqResourceMergeReader.next();
//      } else {
//        return unseqResourceMergeReader.next();
//      }
//    }
//
//    // only has next in batch reader
//    if (hasNextBatch) {
//      TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
//      batchData.next();
//      return timeValuePair;
//    }
//
//    // only has next in point reader
//    if (hasNextPoint) {
//      return unseqResourceMergeReader.next();
//    }
//
//    return null;
//  }

  private boolean hasNextInBatchDataOrBatchReader() throws IOException {
    // has value in batchData
    if (hasCachedBatchData && batchData.hasNext()) {
      return true;
    } else {
      hasCachedBatchData = false;
    }

    // has value in batchReader
    while (seqResourceIterateReader != null && seqResourceIterateReader.hasNextBatch()) {
      batchData = seqResourceIterateReader.nextBatch();
      if (batchData.hasNext()) {
        hasCachedBatchData = true;
        return true;
      }
    }
    return false;
  }


  @Override
  public boolean hasNextBatch() throws IOException {
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    seqResourceIterateReader.close();
    unseqResourceMergeReader.close();
  }
}
