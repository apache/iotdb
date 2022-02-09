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

package org.apache.iotdb.db.query.udf.api;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;

/**
 * User-defined Time-series Generating Function (UDTF)
 *
 * <p>New UDTF classes need to inherit from this UDTF class.
 *
 * <p>Generates a variable number of output data points for a single input row or a single input
 * window (time-based or size-based).
 *
 * <p>A complete UDTF needs to override at least the following methods:
 *
 * <ul>
 *   <li>{@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}
 *   <li>{@link UDTF#transform(RowWindow, PointCollector)} or {@link UDTF#transform(Row,
 *       PointCollector)}
 * </ul>
 *
 * In the life cycle of a UDTF instance, the calling sequence of each method is as follows:
 *
 * <p>1. {@link UDTF#validate(UDFParameterValidator)} 2. {@link UDTF#beforeStart(UDFParameters,
 * UDTFConfigurations)} 3. {@link UDTF#transform(RowWindow, PointCollector)} or {@link
 * UDTF#transform(Row, PointCollector)} 4. {@link UDTF#terminate(PointCollector)} 5. {@link
 * UDTF#beforeDestroy()}
 *
 * <p>The query engine will instantiate an independent UDTF instance for each udf query column, and
 * different UDTF instances will not affect each other.
 */
public interface UDTF extends UDF {

  /**
   * This method is mainly used to customize UDTF. In this method, the user can do the following
   * things:
   *
   * <ul>
   *   <li>Use UDFParameters to get the time series paths and parse key-value pair attributes
   *       entered by the user.
   *   <li>Set the strategy to access the original data and set the output data type in
   *       UDTFConfigurations.
   *   <li>Create resources, such as establishing external connections, opening files, etc.
   * </ul>
   *
   * <p>This method is called after the UDTF is instantiated and before the beginning of the
   * transformation process.
   *
   * @param parameters used to parse the input parameters entered by the user
   * @param configurations used to set the required properties in the UDTF
   * @throws Exception the user can throw errors if necessary
   */
  @SuppressWarnings("squid:S112")
  void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception;

  /**
   * When the user specifies {@link RowByRowAccessStrategy} to access the original data in {@link
   * UDTFConfigurations}, this method will be called to process the transformation. In a single UDF
   * query, this method may be called multiple times.
   *
   * @param row original input data row (aligned by time)
   * @param collector used to collect output data points
   * @throws Exception the user can throw errors if necessary
   * @see RowByRowAccessStrategy
   */
  @SuppressWarnings("squid:S112")
  default void transform(Row row, PointCollector collector) throws Exception {}

  /**
   * When the user specifies {@link SlidingSizeWindowAccessStrategy} or {@link
   * SlidingTimeWindowAccessStrategy} to access the original data in {@link UDTFConfigurations},
   * this method will be called to process the transformation. In a single UDF query, this method
   * may be called multiple times.
   *
   * @param rowWindow original input data window (rows inside the window are aligned by time)
   * @param collector used to collect output data points
   * @throws Exception the user can throw errors if necessary
   * @see SlidingSizeWindowAccessStrategy
   * @see SlidingTimeWindowAccessStrategy
   */
  @SuppressWarnings("squid:S112")
  default void transform(RowWindow rowWindow, PointCollector collector) throws Exception {}

  /**
   * This method will be called once after all {@link UDTF#transform(Row, PointCollector) calls or
   * {@link UDTF#transform(RowWindow, PointCollector) calls have been executed. In a single UDF
   * query, this method will and will only be called once.
   *
   * @param collector used to collect output data points
   * @throws Exception the user can throw errors if necessary
   */
  @SuppressWarnings("squid:S112")
  default void terminate(PointCollector collector) throws Exception {}
}
