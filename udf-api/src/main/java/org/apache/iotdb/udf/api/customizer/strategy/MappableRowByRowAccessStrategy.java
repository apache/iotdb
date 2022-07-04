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

package org.apache.iotdb.udf.api.customizer.strategy;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

/**
 * Used in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
 * <p>
 * When the access strategy of a UDTF is set to an instance of this class, the method {@link
 * UDTF#transform(Row)} of the UDTF will be called to transform the original data.
 * You need to override the method in your own UDTF class.
 * <p>
 * Each call of the method {@link UDTF#transform(Row)} processes only one row
 * (aligned by time) of the original data and can generate any number of data points.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
 *   configurations
 *       .setOutputDataType(TSDataType.INT64)
 *       .setAccessStrategy(new MappableRowByRowAccessStrategy());
 * }</pre>
 *
 * @see UDTF
 * @see UDTFConfigurations
 */
public class MappableRowByRowAccessStrategy implements AccessStrategy {
  @Override
  public void check() {
    // nothing needs to check
  }

  @Override
  public AccessStrategyType getAccessStrategyType() {
    return AccessStrategyType.MAPPABLE_ROW_BY_ROW;
  }
}
