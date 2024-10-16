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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.file.metadata.statistics.Statistics;

public class LastByAccumulator implements TableAccumulator {

  @Override
  public long getEstimatedSize() {
    return 0;
  }

  @Override
  public TableAccumulator copy() {
    return null;
  }

  @Override
  public void addInput(Column[] arguments) {}

  @Override
  public void addIntermediate(Column argument) {}

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {}

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {}

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {}

  @Override
  public void reset() {}
}
