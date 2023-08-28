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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;

public class UDTFBottomK extends UDTFSelectK {

  @Override
  protected void constructPQ() throws UDFInputSeriesDataTypeNotValidException {
    switch (dataType) {
      case INT32:
        intPQ = new PriorityQueue<>(k, Comparator.comparing(o -> -o.right));
        break;
      case INT64:
        longPQ = new PriorityQueue<>(k, Comparator.comparing(o -> -o.right));
        break;
      case FLOAT:
        floatPQ = new PriorityQueue<>(k, Comparator.comparing(o -> -o.right));
        break;
      case DOUBLE:
        doublePQ = new PriorityQueue<>(k, Comparator.comparing(o -> -o.right));
        break;
      case TEXT:
        stringPQ =
            new PriorityQueue<>(
                k,
                (pairA, pairB) -> {
                  final String cs1 = pairB.right;
                  final String cs2 = pairA.right;

                  if (Objects.requireNonNull(cs1).equals(Objects.requireNonNull(cs2))) {
                    return 0;
                  }

                  for (int i = 0, len = Math.min(cs1.length(), cs2.length()); i < len; i++) {
                    final char a = cs1.charAt(i);
                    final char b = cs2.charAt(i);
                    if (a != b) {
                      return a - b;
                    }
                  }

                  return cs1.length() - cs2.length();
                });
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE,
            Type.TEXT);
    }
  }

  @Override
  protected void transformInt(long time, int value) {
    if (intPQ.size() < k) {
      intPQ.add(new Pair<>(time, value));
    } else if (value < intPQ.peek().right) {
      intPQ.poll();
      intPQ.add(new Pair<>(time, value));
    }
  }

  @Override
  protected void transformLong(long time, long value) {
    if (longPQ.size() < k) {
      longPQ.add(new Pair<>(time, value));
    } else if (value < longPQ.peek().right) {
      longPQ.poll();
      longPQ.add(new Pair<>(time, value));
    }
  }

  @Override
  protected void transformFloat(long time, float value) {
    if (floatPQ.size() < k) {
      floatPQ.add(new Pair<>(time, value));
    } else if (value < floatPQ.peek().right) {
      floatPQ.poll();
      floatPQ.add(new Pair<>(time, value));
    }
  }

  @Override
  protected void transformDouble(long time, double value) {
    if (doublePQ.size() < k) {
      doublePQ.add(new Pair<>(time, value));
    } else if (value < doublePQ.peek().right) {
      doublePQ.poll();
      doublePQ.add(new Pair<>(time, value));
    }
  }

  @Override
  protected void transformString(long time, String value) {
    if (stringPQ.size() < k) {
      stringPQ.add(new Pair<>(time, value));
    } else if (value.compareTo(stringPQ.peek().right) < 0) {
      stringPQ.poll();
      stringPQ.add(new Pair<>(time, value));
    }
  }
}
