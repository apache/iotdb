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

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class UDTFTopKDTW implements UDTF {

  protected static final Logger LOGGER = LoggerFactory.getLogger(UDTFTopKDTW.class);

  protected static final double EPS = 1e-6;

  private static final int DEFAULT_BATCH_SIZE = 65536;

  private static final String K = "k";
  private static final String BATCH_SIZE = "batchSize";

  protected static final int COLUMN_S = 0;
  protected static final int COLUMN_P = 1;

  protected int k;
  protected int batchSize;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesNumber(2).validateRequiredAttribute(K);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    k = parameters.getInt(K);
    if (k <= 0) {
      throw new UDFParameterNotValidException("k must be positive");
    }

    batchSize = parameters.getIntOrDefault(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    if (batchSize <= 0) {
      throw new UDFParameterNotValidException("batchSize must be positive");
    }

    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(batchSize))
        .setOutputDataType(Type.TEXT);
  }

  protected double safelyReadDoubleValue(Row row, int columnIndex) throws IOException {
    switch (row.getDataType(columnIndex)) {
      case FLOAT:
        return row.getFloat(columnIndex);
      case DOUBLE:
      default:
        return row.getDouble(columnIndex);
    }
  }
}
