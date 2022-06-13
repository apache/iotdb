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

package org.apache.iotdb.udf.api.customizer.config;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.time.ZoneId;

/**
 * Used in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
 * <p>
 * Supports calling methods in a chain.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
 *   configurations
 *       .setOutputDataType(TSDataType.INT64)
 *       .setAccessStrategy(new RowByRowAccessStrategy());
 * }</pre>
 */
public class UDTFConfigurations extends UDFConfigurations {

  protected final ZoneId zoneId;

  public UDTFConfigurations(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  /**
   * Used to specify the output data type of the UDTF. In other words, the data type you set here
   * determines the type of data that the PointCollector in {@link UDTF#transform(Row,
   * PointCollector)}, {@link UDTF#transform(RowWindow, PointCollector)} or {@link
   * UDTF#terminate(PointCollector)} can receive.
   *
   * @param outputDataType the output data type of the UDTF
   * @return this
   * @see PointCollector
   */
  public UDTFConfigurations setOutputDataType(Type outputDataType) {
    this.outputDataType = outputDataType;
    return this;
  }

  protected AccessStrategy accessStrategy;

  public AccessStrategy getAccessStrategy() {
    return accessStrategy;
  }

  /**
   * Used to specify the strategy for accessing raw query data in UDTF.
   *
   * @param accessStrategy the specified access strategy. it should be an instance of {@link
   *     AccessStrategy}.
   * @return this
   * @see RowByRowAccessStrategy
   * @see SlidingTimeWindowAccessStrategy
   * @see SlidingSizeWindowAccessStrategy
   */
  public UDTFConfigurations setAccessStrategy(AccessStrategy accessStrategy) {
    this.accessStrategy = accessStrategy;
    if (accessStrategy instanceof SlidingTimeWindowAccessStrategy
        && ((SlidingTimeWindowAccessStrategy) accessStrategy).getZoneId() == null) {
      ((SlidingTimeWindowAccessStrategy) accessStrategy).setZoneId(zoneId);
    }
    return this;
  }

  @Override
  public void check() {
    super.check();
    if (accessStrategy == null) {
      throw new RuntimeException("Access strategy is not set.");
    }
    accessStrategy.check();
  }
}
