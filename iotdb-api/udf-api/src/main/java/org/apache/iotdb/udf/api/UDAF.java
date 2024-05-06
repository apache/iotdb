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

package org.apache.iotdb.udf.api;

import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.utils.BitMap;

public interface UDAF extends UDF {
  void beforeStart(UDFParameters parameters, UDAFConfigurations configurations);

  /** Create and initialize state. You may bind some resource in this method. */
  State createState();

  /**
   * Batch update state with data columns. You shall iterate columns and update state with raw
   * values
   *
   * @param state state to be updated
   * @param columns input columns from IoTDB TsBlock, time column is always the last column, the
   *     remaining columns are their parameter value columns
   * @param bitMap define some filtered position in columns
   */
  void addInput(State state, Column[] columns, BitMap bitMap);

  /**
   * Merge two state in execution engine.
   *
   * @param state current state
   * @param rhs right-hand-side state to be merged
   */
  void combineState(State state, State rhs);

  /**
   * Calculate output value from final state
   *
   * @param state final state
   * @param resultValue used to collect output data points
   */
  void outputFinal(State state, ResultValue resultValue);

  /**
   * This method is optional Remove partial state from current state. Implement this method to
   * enable sliding window feature.
   *
   * @param state current state
   * @param removed state to be removed
   */
  default void removeState(State state, State removed) {
    throw new UnsupportedOperationException(getClass().getName());
  }
  ;
}
