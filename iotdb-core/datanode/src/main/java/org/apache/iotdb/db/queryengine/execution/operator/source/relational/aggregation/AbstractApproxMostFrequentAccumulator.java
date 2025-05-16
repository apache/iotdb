/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.SpaceSavingStateFactory;

import com.google.gson.Gson;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.StandardCharsets;

public abstract class AbstractApproxMostFrequentAccumulator<T> implements TableAccumulator {

  protected SpaceSavingStateFactory.SingleSpaceSavingState<T> state =
      SpaceSavingStateFactory.createSingleState();

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    columnBuilder.writeBinary(new Binary(state.getSpaceSaving().serialize()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeBinary(
        new Binary(new Gson().toJson(state.getSpaceSaving().getBuckets()), StandardCharsets.UTF_8));
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException(
        "ApproxMostFrequentAccumulator does not support statistics");
  }

  @Override
  public void reset() {
    state.getSpaceSaving().reset();
  }
}
