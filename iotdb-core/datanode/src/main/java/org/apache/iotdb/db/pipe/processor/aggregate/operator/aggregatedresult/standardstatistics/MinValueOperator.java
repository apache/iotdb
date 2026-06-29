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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics;

import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.CustomizedReadableIntermediateResults;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class MinValueOperator implements AggregatedResultOperator {
  @Override
  public String getName() {
    return "min";
  }

  @Override
  public void configureSystemParameters(final Map<String, String> systemParams) {
    // Do nothing
  }

  @Override
  public Set<String> getDeclaredIntermediateValueNames() {
    return Collections.singleton("min");
  }

  @Override
  public Pair<TSDataType, Object> terminateWindow(
      final TSDataType measurementDataType,
      final CustomizedReadableIntermediateResults intermediateResults) {
    return new Pair<>(measurementDataType, intermediateResults.getObject("min"));
  }
}
