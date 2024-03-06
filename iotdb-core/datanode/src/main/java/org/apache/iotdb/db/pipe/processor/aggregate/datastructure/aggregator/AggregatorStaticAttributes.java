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

package org.apache.iotdb.db.pipe.processor.aggregate.datastructure.aggregator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The static attributes of an aggregator. There shall be a one-to-one match between an aggregator's
 * static attributes and its name.
 */
public class AggregatorStaticAttributes {
  private final String aggregatorName;

  /**
   * The terminateWindowFunction receives a Map to get intermediate results by its names, and
   * produce a characteristic value of this window.
   */
  private final Function<Map<String, Object>, Object> terminateWindowFunction;

  private final TSDataType outputDataType;

  /** The needed intermediate value names */
  private final Set<String> declaredIntermediateValueNames;

  public AggregatorStaticAttributes(
      String aggregatorName,
      Function<Map<String, Object>, Object> terminateWindowFunction,
      TSDataType outputDataType,
      Set<String> declaredIntermediateValueNames) {
    this.aggregatorName = aggregatorName;
    this.terminateWindowFunction = terminateWindowFunction;
    this.outputDataType = outputDataType;
    this.declaredIntermediateValueNames = declaredIntermediateValueNames;
  }

  public String getAggregatorName() {
    return aggregatorName;
  }

  public Function<Map<String, Object>, Object> getTerminateWindowFunction() {
    return terminateWindowFunction;
  }

  public TSDataType getOutputDataType() {
    return outputDataType;
  }

  public Set<String> getDeclaredIntermediateValueNames() {
    return declaredIntermediateValueNames;
  }
}
