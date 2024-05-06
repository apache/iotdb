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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult;

import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.CustomizedReadableIntermediateResults;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.util.Map;
import java.util.Set;

/**
 * The operator to calculate an aggregator. There shall be a one-to-one match between an
 * aggregator's static attributes and its name.
 *
 * <p>Note that there will only be one operator used for calculation and the operator itself is
 * stateless, and therefore doesn't need any ser/de functions.
 */
public interface AggregatedResultOperator {

  /**
   * Return the name of the operator, the name shall be in lower case
   *
   * @return the name of the operator
   */
  String getName();

  /**
   * The system will pass in some parameters (e.g. timestamp precision) to help the inner function
   * correctly operate. This will be called at the very first of the operator's lifecycle.
   *
   * @param systemParams the system parameters
   */
  void configureSystemParameters(Map<String, String> systemParams);

  /** Get the needed intermediate value names of this aggregate result. */
  Set<String> getDeclaredIntermediateValueNames();

  /**
   * terminateWindow receives a Map to get intermediate results by its names, and produce a
   * characteristic value of this window.
   */
  Pair<TSDataType, Object> terminateWindow(
      TSDataType measurementDataType, CustomizedReadableIntermediateResults intermediateResults);
}
