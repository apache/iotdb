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

package org.apache.iotdb.db.query.factory;

import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.aggregation.impl.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/** Easy factory pattern to build AggregateFunction. */
public class AggregateResultFactory {

  private AggregateResultFactory() {}

  /**
   * construct AggregateFunction using factory pattern.
   *
   * @param aggrFuncName function name.
   * @param dataType data type.
   */
  public static AggregateResult getAggrResultByName(
      String aggrFuncName, TSDataType dataType, boolean ascending) {
    if (aggrFuncName == null) {
      throw new IllegalArgumentException("AggregateFunction Name must not be null");
    }

    switch (aggrFuncName.toLowerCase()) {
      case SQLConstant.MIN_TIME:
        return !ascending ? new MinTimeDescAggrResult() : new MinTimeAggrResult();
      case SQLConstant.MAX_TIME:
        return !ascending ? new MaxTimeDescAggrResult() : new MaxTimeAggrResult();
      case SQLConstant.MIN_VALUE:
        return new MinValueAggrResult(dataType);
      case SQLConstant.MAX_VALUE:
        return new MaxValueAggrResult(dataType);
      case SQLConstant.EXTREME:
        return new ExtremeAggrResult(dataType);
      case SQLConstant.COUNT:
        return new CountAggrResult();
      case SQLConstant.AVG:
        return new AvgAggrResult(dataType);
      case SQLConstant.FIRST_VALUE:
        return !ascending
            ? new FirstValueDescAggrResult(dataType)
            : new FirstValueAggrResult(dataType);
      case SQLConstant.SUM:
        return new SumAggrResult(dataType);
      case SQLConstant.LAST_VALUE:
        return !ascending
            ? new LastValueDescAggrResult(dataType)
            : new LastValueAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN:
        return new MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT:
        return new OptimizedMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT_2:
        return new Optimized_2_MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT_3:
        return new Optimized_3_MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT_4:
        return new Optimized_4_MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT_5:
        return new Optimized_5_MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_AMORTIZED:
        return new AmortizedMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_FLOATS:
        return new KLLFloatsMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_AGGRESSIVE:
        return new AggressiveMedianAggrResult(dataType);
        //      case SQLConstant.EXACT_MEDIAN_BITS_BUCKET_STAT:
        //        return new BitsBucketStatMedianAggrResult(dataType);
        //      case SQLConstant.EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER:
        //        return new BitsBucketStatFilterMedianAggrResult(dataType);
        //      case SQLConstant.EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER_AGGRESSIVE:
        //        return new BitsBucketStatFilterAggressiveMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT:
        return new KLLStatMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_SINGLE:
        return new KLLStatSingleAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_FLOATS_SINGLE:
        return new KLLFloatsSingleAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_SINGLE_READ:
        return new KLLStatSingleReadAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_DEBUG:
        return new KLLDebugResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_DEBUG_FULL_READING:
        return new KLLStatDebugFullReadingAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_DEBUG_FULL_READING:
        return new KLLDebugFullReadingAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_DEBUG_PAGE_DEMAND_RATE:
        return new KLLStatDebugPageDemandRateAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_OVERLAP_SINGLE:
        return new KLLStatOverlapSingleAggrResult(dataType);
      case SQLConstant.TDIGEST_STAT_SINGLE:
        return new TDigestStatSingleAggrResult(dataType);
      case SQLConstant.SAMPLING_STAT_SINGLE:
        return new SamplingStatSingleAggrResult(dataType);
      case SQLConstant.STRICT_KLL_STAT_SINGLE:
        return new StrictKLLStatSingleAggrResult(dataType);
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggrFuncName);
    }
  }

  public static AggregateResult getAggrResultByName(String aggrFuncName, TSDataType dataType) {
    if (aggrFuncName == null) {
      throw new IllegalArgumentException("AggregateFunction Name must not be null");
    }

    switch (aggrFuncName.toLowerCase()) {
      case SQLConstant.MIN_TIME:
        return new MinTimeAggrResult();
      case SQLConstant.MAX_TIME:
        return new MaxTimeDescAggrResult();
      case SQLConstant.MIN_VALUE:
        return new MinValueAggrResult(dataType);
      case SQLConstant.MAX_VALUE:
        return new MaxValueAggrResult(dataType);
      case SQLConstant.EXTREME:
        return new ExtremeAggrResult(dataType);
      case SQLConstant.COUNT:
        return new CountAggrResult();
      case SQLConstant.AVG:
        return new AvgAggrResult(dataType);
      case SQLConstant.FIRST_VALUE:
        return new FirstValueAggrResult(dataType);
      case SQLConstant.SUM:
        return new SumAggrResult(dataType);
      case SQLConstant.LAST_VALUE:
        return new LastValueDescAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN:
        return new MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT:
        return new OptimizedMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT_2:
        return new Optimized_2_MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT_3:
        return new Optimized_3_MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT_4:
        return new Optimized_4_MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_OPT_5:
        return new Optimized_5_MedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_AMORTIZED:
        return new AmortizedMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_FLOATS:
        return new KLLFloatsMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_AGGRESSIVE:
        return new AggressiveMedianAggrResult(dataType);
        //      case SQLConstant.EXACT_MEDIAN_BITS_BUCKET_STAT:
        //        return new BitsBucketStatMedianAggrResult(dataType);
        //      case SQLConstant.EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER:
        //        return new BitsBucketStatFilterMedianAggrResult(dataType);
        //      case SQLConstant.EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER_AGGRESSIVE:
        //        return new BitsBucketStatFilterAggressiveMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT:
        return new KLLStatMedianAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_SINGLE:
        return new KLLStatSingleAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_FLOATS_SINGLE:
        return new KLLFloatsSingleAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_SINGLE_READ:
        return new KLLStatSingleReadAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_DEBUG:
        return new KLLDebugResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_DEBUG_FULL_READING:
        return new KLLStatDebugFullReadingAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_DEBUG_FULL_READING:
        return new KLLDebugFullReadingAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_DEBUG_PAGE_DEMAND_RATE:
        return new KLLStatDebugPageDemandRateAggrResult(dataType);
      case SQLConstant.EXACT_MEDIAN_KLL_STAT_OVERLAP_SINGLE:
        return new KLLStatOverlapSingleAggrResult(dataType);
      case SQLConstant.TDIGEST_STAT_SINGLE:
        return new TDigestStatSingleAggrResult(dataType);
      case SQLConstant.SAMPLING_STAT_SINGLE:
        return new SamplingStatSingleAggrResult(dataType);
      case SQLConstant.STRICT_KLL_STAT_SINGLE:
        return new StrictKLLStatSingleAggrResult(dataType);
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggrFuncName);
    }
  }

  public static AggregateResult getAggrResultByType(
      AggregationType aggregationType, TSDataType dataType, boolean ascending) {
    switch (aggregationType) {
      case AVG:
        return new AvgAggrResult(dataType);
      case COUNT:
        return new CountAggrResult();
      case SUM:
        return new SumAggrResult(dataType);
      case FIRST_VALUE:
        return !ascending
            ? new FirstValueDescAggrResult(dataType)
            : new FirstValueAggrResult(dataType);
      case LAST_VALUE:
        return !ascending
            ? new LastValueDescAggrResult(dataType)
            : new LastValueAggrResult(dataType);
      case MAX_TIME:
        return !ascending ? new MaxTimeDescAggrResult() : new MaxTimeAggrResult();
      case MIN_TIME:
        return !ascending ? new MinTimeDescAggrResult() : new MinTimeAggrResult();
      case MAX_VALUE:
        return new MaxValueAggrResult(dataType);
      case MIN_VALUE:
        return new MinValueAggrResult(dataType);
      case EXTREME:
        return new ExtremeAggrResult(dataType);
      case EXACT_MEDIAN:
        return new MedianAggrResult(dataType);
      case EXACT_MEDIAN_OPT:
        return new OptimizedMedianAggrResult(dataType);
      case EXACT_MEDIAN_OPT_2:
        return new Optimized_2_MedianAggrResult(dataType);
      case EXACT_MEDIAN_OPT_3:
        return new Optimized_3_MedianAggrResult(dataType);
      case EXACT_MEDIAN_OPT_4:
        return new Optimized_4_MedianAggrResult(dataType);
      case EXACT_MEDIAN_OPT_5:
        return new Optimized_5_MedianAggrResult(dataType);
      case EXACT_MEDIAN_AMORTIZED:
        return new AmortizedMedianAggrResult(dataType);
      case EXACT_MEDIAN_KLL_FLOATS:
        return new KLLFloatsMedianAggrResult(dataType);
      case EXACT_MEDIAN_AGGRESSIVE:
        return new AggressiveMedianAggrResult(dataType);
        //      case EXACT_MEDIAN_BITS_BUCKET_STAT:
        //        return new BitsBucketStatMedianAggrResult(dataType);
        //      case EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER:
        //        return new BitsBucketStatFilterMedianAggrResult(dataType);
        //      case EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER_AGGRESSIVE:
        //        return new BitsBucketStatFilterAggressiveMedianAggrResult(dataType);
      case EXACT_MEDIAN_KLL_STAT:
        return new KLLStatMedianAggrResult(dataType);
      case EXACT_MEDIAN_KLL_STAT_SINGLE:
        return new KLLStatSingleAggrResult(dataType);
      case EXACT_MEDIAN_KLL_FLOATS_SINGLE:
        return new KLLFloatsSingleAggrResult(dataType);
      case EXACT_MEDIAN_KLL_STAT_SINGLE_READ:
        return new KLLStatSingleReadAggrResult(dataType);
      case EXACT_MEDIAN_KLL_DEBUG:
        return new KLLDebugResult(dataType);
      case EXACT_MEDIAN_KLL_STAT_DEBUG_FULL_READING:
        return new KLLStatDebugFullReadingAggrResult(dataType);
      case EXACT_MEDIAN_KLL_DEBUG_FULL_READING:
        return new KLLDebugFullReadingAggrResult(dataType);
      case EXACT_MEDIAN_KLL_STAT_DEBUG_PAGE_DEMAND_RATE:
        return new KLLStatDebugPageDemandRateAggrResult(dataType);
      case EXACT_MEDIAN_KLL_STAT_OVERLAP_SINGLE:
        return new KLLStatOverlapSingleAggrResult(dataType);
      case TDIGEST_STAT_SINGLE:
        return new TDigestStatSingleAggrResult(dataType);
      case SAMPLING_STAT_SINGLE:
        return new SamplingStatSingleAggrResult(dataType);
      case STRICT_KLL_STAT_SINGLE:
        return new StrictKLLStatSingleAggrResult(dataType);
      default:
        throw new IllegalArgumentException("Invalid Aggregation Type: " + aggregationType.name());
    }
  }
}
