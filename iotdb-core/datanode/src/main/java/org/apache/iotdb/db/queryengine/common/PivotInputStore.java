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

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PivotRelation;
import org.apache.iotdb.commons.queryengine.utils.cte.CteDataStore;
import org.apache.iotdb.db.queryengine.plan.planner.memory.NotThreadSafeMemoryReservationManager;

import java.util.List;

/** A single dynamic PIVOT's materialized input and the values discovered from that same input. */
public final class PivotInputStore {
  private final CteDataStore data;
  private final List<List<PivotRelation.Value>> values;
  private final List<String> inputColumnNames;
  private final int inputColumnCount;
  private final NotThreadSafeMemoryReservationManager reservation;
  private boolean cleared;

  public PivotInputStore(
      CteDataStore data,
      List<List<PivotRelation.Value>> values,
      List<String> inputColumnNames,
      int inputColumnCount,
      NotThreadSafeMemoryReservationManager reservation) {
    this.data = data;
    this.values = values.stream().map(List::copyOf).toList();
    this.inputColumnNames = List.copyOf(inputColumnNames);
    this.inputColumnCount = inputColumnCount;
    this.reservation = reservation;
  }

  public CteDataStore getData() {
    return data;
  }

  public List<List<PivotRelation.Value>> getValues() {
    return values;
  }

  public List<String> getInputColumnNames() {
    return inputColumnNames;
  }

  public int getInputColumnCount() {
    return inputColumnCount;
  }

  public void clear() {
    if (!cleared) {
      cleared = true;
      data.clear();
      reservation.releaseAllReservedMemory();
    }
  }
}
