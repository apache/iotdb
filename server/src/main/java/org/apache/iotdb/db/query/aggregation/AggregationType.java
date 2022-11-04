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
  STRICT_KLL_STAT_SINGLE;

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
      default:
        throw new IllegalArgumentException("Invalid Aggregation Type: " + this.name());
    }

    byte[] bytes = BytesUtils.shortToBytes(i);
    outputStream.write(bytes);
  }
}
