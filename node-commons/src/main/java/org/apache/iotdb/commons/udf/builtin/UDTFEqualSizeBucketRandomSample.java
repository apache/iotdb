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
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.util.Random;

public class UDTFEqualSizeBucketRandomSample extends UDTFEqualSizeBucketSample {

  private Random random;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    random = new Random();
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(bucketSize))
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector)
      throws IOException, UDFInputSeriesDataTypeNotValidException {
    Row row = rowWindow.getRow(random.nextInt(rowWindow.windowSize()));
    switch (dataType) {
      case INT32:
        collector.putInt(row.getTime(), row.getInt(0));
        break;
      case INT64:
        collector.putLong(row.getTime(), row.getLong(0));
        break;
      case FLOAT:
        collector.putFloat(row.getTime(), row.getFloat(0));
        break;
      case DOUBLE:
        collector.putDouble(row.getTime(), row.getDouble(0));
        break;
      default:
        // This will not happen
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }
}
