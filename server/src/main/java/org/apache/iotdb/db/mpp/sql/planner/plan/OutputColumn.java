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
package org.apache.iotdb.db.mpp.sql.planner.plan;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class OutputColumn {

  // indicate this output column should use which value column of which input TsBlock
  // if overlapped is false, the order in sourceLocations should be in ascending timestamp order
  private final List<InputLocation> sourceLocations;

  // if overlapped is true, it means that sourceLocations.size() > 1 and input locations in
  // sourceLocations are overlapped
  // it will only happen when we do the load balance and more than one DataRegion is assigned to one
  // time partition
  private final boolean overlapped;

  /** used for case that this OutputColumn only has one input column */
  public OutputColumn(InputLocation inputLocation) {
    this.sourceLocations = ImmutableList.of(inputLocation);
    this.overlapped = false;
  }

  public OutputColumn(List<InputLocation> sourceLocations, boolean overlapped) {
    this.sourceLocations = sourceLocations;
    this.overlapped = overlapped;
  }

  public List<InputLocation> getSourceLocations() {
    return sourceLocations;
  }

  public boolean isOverlapped() {
    return overlapped;
  }
}
