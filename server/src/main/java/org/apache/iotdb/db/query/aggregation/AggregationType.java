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

import org.apache.iotdb.tsfile.utils.BytesUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public enum AggregationType {
  COUNT,
  AVG,
  SUM,
  FIRST_VALUE,
  LAST_VALUE,
  MAX_TIME,
  MIN_TIME,
  MAX_VALUE,
  MIN_VALUE,
  EXTREME,
  EXACT_MEDIAN,
  EXACT_MEDIAN_OPT,
  EXACT_MEDIAN_OPT_2,
  EXACT_MEDIAN_OPT_3,
  EXACT_MEDIAN_OPT_4,
  EXACT_MEDIAN_OPT_5,
  EXACT_MEDIAN_AMORTIZED,
  EXACT_MEDIAN_KLL_FLOATS,
  EXACT_MEDIAN_AGGRESSIVE,
  EXACT_MEDIAN_BITS_BUCKET_STAT,
  EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER,
  EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER_AGGRESSIVE,
  EXACT_MEDIAN_KLL_STAT,
  EXACT_MEDIAN_KLL_STAT_SINGLE,
  EXACT_MEDIAN_KLL_FLOATS_SINGLE,
  EXACT_MEDIAN_KLL_STAT_SINGLE_READ,
  EXACT_MEDIAN_KLL_DEBUG,
  EXACT_MEDIAN_KLL_STAT_DEBUG_FULL_READING,
  EXACT_MEDIAN_KLL_DEBUG_FULL_READING,
  EXACT_MEDIAN_KLL_STAT_DEBUG_PAGE_DEMAND_RATE,
  EXACT_MEDIAN_KLL_STAT_OVERLAP_SINGLE,
  TDIGEST_STAT_SINGLE,
  SAMPLING_STAT_SINGLE,
  STRICT_KLL_STAT_SINGLE,
  DDSKETCH_SINGLE,
  CHUNK_STAT_AVAIL,
  EXACT_QUANTILE_BASELINE_KLL,
  EXACT_QUANTILE_PR_KLL_NO_OPT,
  EXACT_QUANTILE_PR_KLL_OPT_STAT,
  EXACT_QUANTILE_DDSKETCH,
  EXACT_QUANTILE_PR_KLL_OPT_FILTER,
  EXACT_QUANTILE_PR_KLL_OPT_SUMMARY,
  FULL_READ_ONCE,
  EXACT_QUANTILE_QUICK_SELECT,
  EXACT_MULTI_QUANTILES_QUICK_SELECT,
  EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY,
  EXACT_QUANTILE_PR_KLL_PRIORI_FIX_PR,
  EXACT_QUANTILE_PR_KLL_PRIORI_BEST_PR,
  EXACT_QUANTILE_PR_KLL_POST_BEST_PR,
  EXACT_QUANTILE_MRL,
  EXACT_QUANTILE_TDIGEST,
  EXACT_QUANTILE_DDSKETCH_POSITIVE,
  EXACT_MULTI_QUANTILES_PR_KLL_POST_BEST_PR,
  EXACT_MULTI_QUANTILES_PR_KLL_FIX_PR,
  EXACT_MULTI_QUANTILES_MRL,
  EXACT_MULTI_QUANTILES_TDIGEST,
  EXACT_MULTI_QUANTILES_DDSKETCH_POSITIVE;

  /**
   * give an integer to return a data type.
   *
   * @return -enum type
   */
  public static AggregationType deserialize(ByteBuffer byteBuffer) {
    short i = byteBuffer.getShort();
    switch (i) {
      case 0:
        return COUNT;
      case 1:
        return AVG;
      case 2:
        return SUM;
      case 3:
        return FIRST_VALUE;
      case 4:
        return LAST_VALUE;
      case 5:
        return MAX_TIME;
      case 6:
        return MIN_TIME;
      case 7:
        return MAX_VALUE;
      case 8:
        return MIN_VALUE;
      case 9:
        return EXTREME;
      case 10:
        return EXACT_MEDIAN;
      case 11:
        return EXACT_MEDIAN_OPT;
      case 12:
        return EXACT_MEDIAN_OPT_2;
      case 13:
        return EXACT_MEDIAN_OPT_3;
      case 14:
        return EXACT_MEDIAN_OPT_4;
      case 15:
        return EXACT_MEDIAN_OPT_5;
      case 16:
        return EXACT_MEDIAN_AMORTIZED;
      case 17:
        return EXACT_MEDIAN_KLL_FLOATS;
      case 18:
        return EXACT_MEDIAN_AGGRESSIVE;
      case 19:
        return EXACT_MEDIAN_BITS_BUCKET_STAT;
      case 20:
        return EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER;
      case 21:
        return EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER_AGGRESSIVE;
      case 22:
        return EXACT_MEDIAN_KLL_STAT;
      case 23:
        return EXACT_MEDIAN_KLL_STAT_SINGLE;
      case 24:
        return EXACT_MEDIAN_KLL_FLOATS_SINGLE;
      case 25:
        return EXACT_MEDIAN_KLL_STAT_SINGLE_READ;
      case 26:
        return EXACT_MEDIAN_KLL_DEBUG;
      case 27:
        return EXACT_MEDIAN_KLL_STAT_DEBUG_FULL_READING;
      case 28:
        return EXACT_MEDIAN_KLL_DEBUG_FULL_READING;
      case 29:
        return EXACT_MEDIAN_KLL_STAT_DEBUG_PAGE_DEMAND_RATE;
      case 30:
        return EXACT_MEDIAN_KLL_STAT_OVERLAP_SINGLE;
      case 31:
        return TDIGEST_STAT_SINGLE;
      case 32:
        return SAMPLING_STAT_SINGLE;
      case 33:
        return STRICT_KLL_STAT_SINGLE;
      case 34:
        return DDSKETCH_SINGLE;
      case 35:
        return CHUNK_STAT_AVAIL;
      case 36:
        return EXACT_QUANTILE_BASELINE_KLL;
      case 37:
        return EXACT_QUANTILE_PR_KLL_NO_OPT;
      case 38:
        return EXACT_QUANTILE_DDSKETCH;
      case 39:
        return EXACT_QUANTILE_PR_KLL_OPT_STAT;
      case 40:
        return EXACT_QUANTILE_PR_KLL_OPT_FILTER;
      case 41:
        return EXACT_QUANTILE_PR_KLL_OPT_SUMMARY;
      case 42:
        return FULL_READ_ONCE;
      case 43:
        return EXACT_QUANTILE_QUICK_SELECT;
      case 44:
        return EXACT_MULTI_QUANTILES_QUICK_SELECT;
      case 45:
        return EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY;
      case 46:
        return EXACT_QUANTILE_PR_KLL_PRIORI_FIX_PR;
      case 47:
        return EXACT_QUANTILE_PR_KLL_PRIORI_BEST_PR;
      case 48:
        return EXACT_QUANTILE_PR_KLL_POST_BEST_PR;
      case 49:
        return EXACT_QUANTILE_MRL;
      case 50:
        return EXACT_QUANTILE_TDIGEST;
      case 51:
        return EXACT_QUANTILE_DDSKETCH_POSITIVE;
      case 52:
        return EXACT_MULTI_QUANTILES_PR_KLL_POST_BEST_PR;
      case 53:
        return EXACT_MULTI_QUANTILES_PR_KLL_FIX_PR;
      case 54:
        return EXACT_MULTI_QUANTILES_MRL;
      case 55:
        return EXACT_MULTI_QUANTILES_TDIGEST;
      case 56:
        return EXACT_MULTI_QUANTILES_DDSKETCH_POSITIVE;

      default:
        throw new IllegalArgumentException("Invalid Aggregation Type: " + i);
    }
  }

  public void serializeTo(OutputStream outputStream) throws IOException {
    short i;
    switch (this) {
      case COUNT:
        i = 0;
        break;
      case AVG:
        i = 1;
        break;
      case SUM:
        i = 2;
        break;
      case FIRST_VALUE:
        i = 3;
        break;
      case LAST_VALUE:
        i = 4;
        break;
      case MAX_TIME:
        i = 5;
        break;
      case MIN_TIME:
        i = 6;
        break;
      case MAX_VALUE:
        i = 7;
        break;
      case MIN_VALUE:
        i = 8;
        break;
      case EXTREME:
        i = 9;
        break;
      case EXACT_MEDIAN:
        i = 10;
        break;
      case EXACT_MEDIAN_OPT:
        i = 11;
        break;
      case EXACT_MEDIAN_OPT_2:
        i = 12;
        break;
      case EXACT_MEDIAN_OPT_3:
        i = 13;
        break;
      case EXACT_MEDIAN_OPT_4:
        i = 14;
        break;
      case EXACT_MEDIAN_OPT_5:
        i = 15;
        break;
      case EXACT_MEDIAN_AMORTIZED:
        i = 16;
        break;
      case EXACT_MEDIAN_KLL_FLOATS:
        i = 17;
        break;
      case EXACT_MEDIAN_AGGRESSIVE:
        i = 18;
        break;
      case EXACT_MEDIAN_BITS_BUCKET_STAT:
        i = 19;
        break;
      case EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER:
        i = 20;
        break;
      case EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER_AGGRESSIVE:
        i = 21;
        break;
      case EXACT_MEDIAN_KLL_STAT:
        i = 22;
        break;
      case EXACT_MEDIAN_KLL_STAT_SINGLE:
        i = 23;
        break;
      case EXACT_MEDIAN_KLL_FLOATS_SINGLE:
        i = 24;
        break;
      case EXACT_MEDIAN_KLL_STAT_SINGLE_READ:
        i = 25;
        break;
      case EXACT_MEDIAN_KLL_DEBUG:
        i = 26;
        break;
      case EXACT_MEDIAN_KLL_STAT_DEBUG_FULL_READING:
        i = 27;
        break;
      case EXACT_MEDIAN_KLL_DEBUG_FULL_READING:
        i = 28;
        break;
      case EXACT_MEDIAN_KLL_STAT_DEBUG_PAGE_DEMAND_RATE:
        i = 29;
        break;
      case EXACT_MEDIAN_KLL_STAT_OVERLAP_SINGLE:
        i = 30;
        break;
      case TDIGEST_STAT_SINGLE:
        i = 31;
        break;
      case SAMPLING_STAT_SINGLE:
        i = 32;
        break;
      case STRICT_KLL_STAT_SINGLE:
        i = 33;
        break;
      case DDSKETCH_SINGLE:
        i = 34;
        break;
      case CHUNK_STAT_AVAIL:
        i = 35;
        break;
      case EXACT_QUANTILE_BASELINE_KLL:
        i = 36;
        break;
      case EXACT_QUANTILE_PR_KLL_NO_OPT:
        i = 37;
        break;
      case EXACT_QUANTILE_DDSKETCH:
        i = 38;
        break;
      case EXACT_QUANTILE_PR_KLL_OPT_STAT:
        i = 39;
        break;
      case EXACT_QUANTILE_PR_KLL_OPT_FILTER:
        i = 40;
        break;
      case EXACT_QUANTILE_PR_KLL_OPT_SUMMARY:
        i = 41;
        break;
      case FULL_READ_ONCE:
        i = 42;
        break;
      case EXACT_QUANTILE_QUICK_SELECT:
        i = 43;
        break;
      case EXACT_MULTI_QUANTILES_QUICK_SELECT:
        i = 44;
        break;
      case EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY:
        i = 45;
        break;
      case EXACT_QUANTILE_PR_KLL_PRIORI_FIX_PR:
        i = 46;
        break;
      case EXACT_QUANTILE_PR_KLL_PRIORI_BEST_PR:
        i = 47;
        break;
      case EXACT_QUANTILE_PR_KLL_POST_BEST_PR:
        i = 48;
        break;
      case EXACT_QUANTILE_MRL:
        i = 49;
        break;
      case EXACT_QUANTILE_TDIGEST:
        i = 50;
        break;
      case EXACT_QUANTILE_DDSKETCH_POSITIVE:
        i = 51;
        break;
      case EXACT_MULTI_QUANTILES_PR_KLL_POST_BEST_PR:
        i = 52;
        break;
      case EXACT_MULTI_QUANTILES_PR_KLL_FIX_PR:
        i = 53;
        break;
      case EXACT_MULTI_QUANTILES_MRL:
        i = 54;
        break;
      case EXACT_MULTI_QUANTILES_TDIGEST:
        i = 55;
        break;
      case EXACT_MULTI_QUANTILES_DDSKETCH_POSITIVE:
        i = 56;
        break;
      default:
        throw new IllegalArgumentException("Invalid Aggregation Type: " + this.name());
    }

    byte[] bytes = BytesUtils.shortToBytes(i);
    outputStream.write(bytes);
  }
}
