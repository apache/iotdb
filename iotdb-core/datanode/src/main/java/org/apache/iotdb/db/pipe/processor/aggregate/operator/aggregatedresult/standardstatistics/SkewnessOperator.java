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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SkewnessOperator implements AggregatedResultOperator {
  @Override
  public String getName() {
    return "skew";
  }

  @Override
  public void configureSystemParameters(Map<String, String> systemParams) {
    // Do nothing
  }

  @Override
  public Set<String> getDeclaredIntermediateValueNames() {
    return Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("sum_x3", "sum_x2", "sum_x1", "count")));
  }

  @Override
  public Pair<TSDataType, Object> terminateWindow(
      TSDataType measurementDataType, CustomizedReadableIntermediateResults intermediateResults) {
    double sumX3 = intermediateResults.getDouble("sum_x3");
    double sumX2 = intermediateResults.getDouble("sum_x2");
    double sumX1 = intermediateResults.getDouble("sum_x1");
    int count = intermediateResults.getInt("count");

    return new Pair<>(
        TSDataType.DOUBLE,
        (sumX3 / count - 3 * sumX1 / count * sumX2 / count + 2 * Math.pow(sumX1 / count, 3))
            / Math.pow(sumX2 / count - Math.pow(sumX1 / count, 2), 1.5));
  }
}
