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
  EXTREME;

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
      default:
        throw new IllegalArgumentException("Invalid Aggregation Type: " + this.name());
    }

    byte[] bytes = BytesUtils.shortToBytes(i);
    outputStream.write(bytes);
  }
}
