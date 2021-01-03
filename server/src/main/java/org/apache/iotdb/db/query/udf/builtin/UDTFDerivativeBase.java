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

package org.apache.iotdb.db.query.udf.builtin;

import java.io.IOException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class UDTFDerivativeBase implements UDTF {

  private boolean hasPrevious = false;

  protected long previousTime = 0;

  protected int previousInt = 0;
  protected long previousLong = 0;
  protected float previousFloat = 0;
  protected double previousDouble = 0;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
            TSDataType.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    if (!hasPrevious) {
      updatePrevious(row);
      hasPrevious = true;
      return;
    }

    doTransform(row, collector);
  }

  private void updatePrevious(Row row) throws UDFInputSeriesDataTypeNotValidException {
    previousTime = row.getTime();
    switch (row.getDataType(0)) {
      case INT32:
        previousInt = row.getInt(0);
        break;
      case INT64:
        previousLong = row.getLong(0);
        break;
      case FLOAT:
        previousFloat = row.getFloat(0);
        break;
      case DOUBLE:
        previousDouble = row.getDouble(0);
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(0, row.getDataType(0), TSDataType.INT32,
            TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  protected abstract void doTransform(Row row, PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException;
}
