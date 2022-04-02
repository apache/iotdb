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
package org.apache.iotdb.confignode.partition;

import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * DataPartitionRule is used to hold real-time write-load allocation rules i.e. rules = [(0, 0.3),
 * (1, 0.2), (2. 0.5)] means allocate 30% of the write-load to DataRegion-0 and 20% to DataRegion-1
 * and 50% to DataRegion-2
 */
public class DataPartitionRule {
  // List<Pair<DataRegionID, Write allocation ratio>>
  private final List<Pair<Integer, Double>> rules;

  public DataPartitionRule() {
    this.rules = new ArrayList<>();
  }

  public DataPartitionRule(List<Pair<Integer, Double>> rules) {
    this.rules = rules;
  }

  public void addDataPartitionRule(int dataRegionID, double ratio) {
    this.rules.add(new Pair<>(dataRegionID, ratio));
  }

  public List<Pair<Integer, Double>> getDataPartitionRule() {
    return this.rules;
  }
}
