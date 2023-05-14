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
      case SQLConstant.DDSKETCH_SINGLE:
        return new DDSketchSingleAggrResult(dataType);
      case SQLConstant.CHUNK_STAT_AVAIL:
        return new KLLStatChunkAvailAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_BASELINE_KLL:
        return new ExactQuantileBaselineKLLAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_NO_OPT:
        return new ExactQuantilePrKLLNoOptAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_DDSKETCH:
        return new ExactQuantileDDSketchAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_OPT_STAT:
        return new ExactQuantilePrKLLOptStatAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_OPT_FILTER:
        return new ExactQuantilePrKLLOptFilterAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_OPT_SUMMARY:
        return new ExactQuantilePrKLLOptSummaryAggrResult(dataType);
      case SQLConstant.FULL_READ_ONCE:
        return new FullReadOnceAggrResult();
      case SQLConstant.EXACT_QUANTILE_QUICK_SELECT:
        return new ExactQuantileQuickSelectAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_QUICK_SELECT:
        return new ExactMultiQuantilesQuickSelectAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY:
        return new ExactMultiQuantilesPrKLLOptSummaryAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_PRIORI_FIX_PR:
        return new ExactQuantilePrKLLPrioriFixPrAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_PRIORI_BEST_PR:
        return new ExactQuantilePrKLLPrioriBestPrAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_POST_BEST_PR:
        return new ExactQuantilePrKLLPostBestPrAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_MRL:
        return new ExactQuantileMRLAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_TDIGEST:
        return new ExactQuantileTDigestAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_DDSKETCH_POSITIVE:
        return new ExactQuantileDDSketchPositiveAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_PR_KLL_POST_BEST_PR:
        return new ExactMultiQuantilesPrKLLPostBestPrAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_PR_KLL_FIX_PR:
        return new ExactMultiQuantilesPrKLLFixPrAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_MRL:
        return new ExactMultiQuantilesMRLAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_TDIGEST:
        return new ExactMultiQuantilesTDigestAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_DDSKETCH_POSITIVE:
        return new ExactMultiQuantilesDDSketchAggrResult(dataType);
      case SQLConstant.MAD_DD:
        return new MADDDAggrResult(dataType);
      case SQLConstant.MAD_CORE:
        return new MADCoreAggrResult(dataType);
      case SQLConstant.MAD_QS:
        return new MADQSAggrResult(dataType);
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
      case SQLConstant.DDSKETCH_SINGLE:
        return new DDSketchSingleAggrResult(dataType);
      case SQLConstant.CHUNK_STAT_AVAIL:
        return new KLLStatChunkAvailAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_BASELINE_KLL:
        return new ExactQuantileBaselineKLLAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_NO_OPT:
        return new ExactQuantilePrKLLNoOptAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_DDSKETCH:
        return new ExactQuantileDDSketchAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_OPT_STAT:
        return new ExactQuantilePrKLLOptStatAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_OPT_FILTER:
        return new ExactQuantilePrKLLOptFilterAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_OPT_SUMMARY:
        return new ExactQuantilePrKLLOptSummaryAggrResult(dataType);
      case SQLConstant.FULL_READ_ONCE:
        return new FullReadOnceAggrResult();
      case SQLConstant.EXACT_QUANTILE_QUICK_SELECT:
        return new ExactQuantileQuickSelectAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_QUICK_SELECT:
        return new ExactMultiQuantilesQuickSelectAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY:
        return new ExactMultiQuantilesPrKLLOptSummaryAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_PRIORI_FIX_PR:
        return new ExactQuantilePrKLLPrioriFixPrAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_PRIORI_BEST_PR:
        return new ExactQuantilePrKLLPrioriBestPrAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_PR_KLL_POST_BEST_PR:
        return new ExactQuantilePrKLLPostBestPrAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_MRL:
        return new ExactQuantileMRLAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_TDIGEST:
        return new ExactQuantileTDigestAggrResult(dataType);
      case SQLConstant.EXACT_QUANTILE_DDSKETCH_POSITIVE:
        return new ExactQuantileDDSketchPositiveAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_PR_KLL_POST_BEST_PR:
        return new ExactMultiQuantilesPrKLLPostBestPrAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_PR_KLL_FIX_PR:
        return new ExactMultiQuantilesPrKLLFixPrAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_MRL:
        return new ExactMultiQuantilesMRLAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_TDIGEST:
        return new ExactMultiQuantilesTDigestAggrResult(dataType);
      case SQLConstant.EXACT_MULTI_QUANTILES_DDSKETCH_POSITIVE:
        return new ExactMultiQuantilesDDSketchAggrResult(dataType);
      case SQLConstant.MAD_DD:
        return new MADDDAggrResult(dataType);
      case SQLConstant.MAD_CORE:
        return new MADCoreAggrResult(dataType);
      case SQLConstant.MAD_QS:
        return new MADQSAggrResult(dataType);
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
      case DDSKETCH_SINGLE:
        return new DDSketchSingleAggrResult(dataType);
      case CHUNK_STAT_AVAIL:
        return new KLLStatChunkAvailAggrResult(dataType);
      case EXACT_QUANTILE_BASELINE_KLL:
        return new ExactQuantileBaselineKLLAggrResult(dataType);
      case EXACT_QUANTILE_PR_KLL_NO_OPT:
        return new ExactQuantilePrKLLNoOptAggrResult(dataType);
      case EXACT_QUANTILE_DDSKETCH:
        return new ExactQuantileDDSketchAggrResult(dataType);
      case EXACT_QUANTILE_PR_KLL_OPT_STAT:
        return new ExactQuantilePrKLLOptStatAggrResult(dataType);
      case EXACT_QUANTILE_PR_KLL_OPT_FILTER:
        return new ExactQuantilePrKLLOptFilterAggrResult(dataType);
      case EXACT_QUANTILE_PR_KLL_OPT_SUMMARY:
        return new ExactQuantilePrKLLOptSummaryAggrResult(dataType);
      case FULL_READ_ONCE:
        return new FullReadOnceAggrResult();
      case EXACT_QUANTILE_QUICK_SELECT:
        return new ExactQuantileQuickSelectAggrResult(dataType);
      case EXACT_MULTI_QUANTILES_QUICK_SELECT:
        return new ExactMultiQuantilesQuickSelectAggrResult(dataType);
      case EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY:
        return new ExactMultiQuantilesPrKLLOptSummaryAggrResult(dataType);
      case EXACT_QUANTILE_PR_KLL_PRIORI_FIX_PR:
        return new ExactQuantilePrKLLPrioriFixPrAggrResult(dataType);
      case EXACT_QUANTILE_PR_KLL_PRIORI_BEST_PR:
        return new ExactQuantilePrKLLPrioriBestPrAggrResult(dataType);
      case EXACT_QUANTILE_PR_KLL_POST_BEST_PR:
        return new ExactQuantilePrKLLPostBestPrAggrResult(dataType);
      case EXACT_QUANTILE_MRL:
        return new ExactQuantileMRLAggrResult(dataType);
      case EXACT_QUANTILE_TDIGEST:
        return new ExactQuantileTDigestAggrResult(dataType);
      case EXACT_QUANTILE_DDSKETCH_POSITIVE:
        return new ExactQuantileDDSketchPositiveAggrResult(dataType);
      case EXACT_MULTI_QUANTILES_PR_KLL_POST_BEST_PR:
        return new ExactMultiQuantilesPrKLLPostBestPrAggrResult(dataType);
      case EXACT_MULTI_QUANTILES_PR_KLL_FIX_PR:
        return new ExactMultiQuantilesPrKLLFixPrAggrResult(dataType);
      case EXACT_MULTI_QUANTILES_MRL:
        return new ExactMultiQuantilesMRLAggrResult(dataType);
      case EXACT_MULTI_QUANTILES_TDIGEST:
        return new ExactMultiQuantilesTDigestAggrResult(dataType);
      case EXACT_MULTI_QUANTILES_DDSKETCH_POSITIVE:
        return new ExactMultiQuantilesDDSketchAggrResult(dataType);
      case MAD_DD:
        return new MADDDAggrResult(dataType);
      case MAD_CORE:
        return new MADCoreAggrResult(dataType);
      case MAD_QS:
        return new MADQSAggrResult(dataType);
      default:
        throw new IllegalArgumentException("Invalid Aggregation Type: " + aggregationType.name());
    }
  }
}
