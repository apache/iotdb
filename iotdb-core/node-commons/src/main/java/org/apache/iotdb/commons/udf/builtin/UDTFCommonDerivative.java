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
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

public class UDTFCommonDerivative extends UDTFDerivative {

  @Override
  protected void doTransform(Row row, PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    long currentTime = row.getTime();
    double timeDelta = (double) currentTime - previousTime;
    switch (dataType) {
      case INT32:
        int currentInt = row.getInt(0);
        collector.putDouble(currentTime, (currentInt - previousInt) / timeDelta);
        previousInt = currentInt;
        break;
      case INT64:
        long currentLong = row.getLong(0);
        collector.putDouble(currentTime, (currentLong - previousLong) / timeDelta);
        previousLong = currentLong;
        break;
      case FLOAT:
        float currentFloat = row.getFloat(0);
        collector.putDouble(currentTime, (currentFloat - previousFloat) / timeDelta);
        previousFloat = currentFloat;
        break;
      case DOUBLE:
        double currentDouble = row.getDouble(0);
        collector.putDouble(currentTime, (currentDouble - previousDouble) / timeDelta);
        previousDouble = currentDouble;
        break;
      case DATE:
      case BOOLEAN:
      case TIMESTAMP:
      case TEXT:
      case STRING:
      case BLOB:
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
    previousTime = currentTime;
  }
}
