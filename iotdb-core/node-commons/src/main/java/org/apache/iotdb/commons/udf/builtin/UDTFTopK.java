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
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.utils.Pair;

import java.util.Comparator;
import java.util.PriorityQueue;

public class UDTFTopK extends UDTFSelectK {

  @Override
  protected void constructPQ() throws UDFInputSeriesDataTypeNotValidException {
    switch (dataType) {
      case INT32:
      case DATE:
        intPQ = new PriorityQueue<>(k, Comparator.comparing(o -> o.right));
        break;
      case INT64:
      case TIMESTAMP:
        longPQ = new PriorityQueue<>(k, Comparator.comparing(o -> o.right));
        break;
      case FLOAT:
        floatPQ = new PriorityQueue<>(k, Comparator.comparing(o -> o.right));
        break;
      case DOUBLE:
        doublePQ = new PriorityQueue<>(k, Comparator.comparing(o -> o.right));
        break;
      case TEXT:
      case STRING:
        stringPQ = new PriorityQueue<>(k, Comparator.comparing(o -> o.right));
        break;
      case BOOLEAN:
      case BLOB:
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE,
            Type.TEXT,
            Type.DATE,
            Type.TIMESTAMP,
            Type.STRING);
    }
  }

  @Override
  protected void transformInt(long time, int value) {
    if (intPQ.size() < k) {
      intPQ.add(new Pair<>(time, value));
    } else if (intPQ.peek().right < value) {
      intPQ.poll();
      intPQ.add(new Pair<>(time, value));
    }
  }

  @Override
  protected void transformLong(long time, long value) {
    if (longPQ.size() < k) {
      longPQ.add(new Pair<>(time, value));
    } else if (longPQ.peek().right < value) {
      longPQ.poll();
      longPQ.add(new Pair<>(time, value));
    }
  }

  @Override
  protected void transformFloat(long time, float value) {
    if (floatPQ.size() < k) {
      floatPQ.add(new Pair<>(time, value));
    } else if (floatPQ.peek().right < value) {
      floatPQ.poll();
      floatPQ.add(new Pair<>(time, value));
    }
  }

  @Override
  protected void transformDouble(long time, double value) {
    if (doublePQ.size() < k) {
      doublePQ.add(new Pair<>(time, value));
    } else if (doublePQ.peek().right < value) {
      doublePQ.poll();
      doublePQ.add(new Pair<>(time, value));
    }
  }

  @Override
  protected void transformString(long time, String value) {
    if (stringPQ.size() < k) {
      stringPQ.add(new Pair<>(time, value));
    } else if (stringPQ.peek().right.compareTo(value) < 0) {
      stringPQ.poll();
      stringPQ.add(new Pair<>(time, value));
    }
  }
}
