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

package org.apache.iotdb.db.query.aggregation;

import java.io.IOException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public abstract class AggregateResult {

  protected AggreResultData resultData;

  /**
   * construct.
   *
   * @param dataType result data type.
   */
  public AggregateResult(TSDataType dataType) {
    this.resultData = new AggreResultData(dataType);
  }

  public abstract void init();

  public abstract AggreResultData getResult();

  /**
   * Calculate the aggregation using Statistics
   *
   * @param statistics chunkStatistics or pageStatistics
   */
  public abstract void updateResultFromStatistics(Statistics statistics)
      throws QueryProcessException;

  /**
   * Aggregate results cannot be calculated using Statistics directly, using the data in each page
   *
   * @param dataInThisPage the data in Page
   */
  public abstract void updateResultFromPageData(BatchData dataInThisPage) throws IOException;

  /**
   * Aggregate results cannot be calculated using Statistics directly, using the data in each page
   *
   * @param dataInThisPage the data in Page
   * @param bound calculate points whose time < bound
   */
  public abstract void updateResultFromPageData(BatchData dataInThisPage, long bound)
      throws IOException;

  /**
   * <p> This method is calculate the aggregation using the common timestamps of cross series
   * filter. </p>
   *
   * @throws IOException TsFile data read error
   */
  public abstract void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException;

  /**
   * Judge if aggregation results have been calculated. In other words, if the aggregated result
   * does not need to compute the remaining data, it returns true.
   *
   * @return If the aggregation result has been calculated return true, else return false.
   */
  public abstract boolean isCalculatedAggregationResult();
}
